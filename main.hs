import Data.List.Split
import Data.List
import Control.Exception
import Data.Text (pack, unpack)

import System.Fuse
import Foreign.C.Error
import System.Posix.Types
import System.Posix.Files
import System.Posix.IO

import System.Process

import qualified Data.Aeson as Aeson

import qualified Data.ByteString.Char8 as B
import Data.ByteString.Lazy.UTF8 (fromString, toString)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import qualified Data.Maybe as Maybe

type Condition = (String, String)
data Request = AttrList | AttrVals String | View String [Condition] deriving (Show)

data AnnexFile = AnnexFile {
    file :: String,
    fields :: HashMap.HashMap String [String]
} deriving (Show)

instance Aeson.FromJSON AnnexFile where
 parseJSON (Aeson.Object v) =
    AnnexFile <$> v Aeson..: pack "file"
           <*> v Aeson..: pack "fields"

allFolderName = "all"
pathSep = "/"
repo = "/path/to/repo"

parsePath :: String -> Request
parsePath path =
    let
        parts = filter (\part -> part /= "") (splitOn pathSep path)
        (req, _) = foldl parseNext (AttrList, []) parts
    in
        req

parseNext :: (Request, [Condition]) -> String -> (Request, [Condition])
parseNext (current, conditions) str =
    case current of
        AttrList ->
            if str == allFolderName then
                (View "" conditions, [])
            else
                (AttrVals str, conditions)
        AttrVals key ->
            let
                newConds = (key, str):conditions
            in
                (AttrList, newConds)
        View path conditions ->
            (View (path ++ (if path /= "" then pathSep else "") ++ str) conditions, [])

type HT = ()

helloGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
helloGetFileSystemStats str =
    return $ Right $ FileSystemStats {
        fsStatBlockSize = 512,
        fsStatBlockCount = 1,
        fsStatBlocksFree = 1,
        fsStatBlocksAvailable = 1,
        fsStatFileCount = 5,
        fsStatFilesFree = 10,
        fsStatMaxNameLength = 255
    }

dirStat ctx = FileStat {
    statEntryType = Directory,
    statFileMode = foldr1 unionFileModes [
        ownerReadMode,
        ownerExecuteMode
    ],
    statLinkCount = 2,
    statFileOwner = fuseCtxUserID ctx,
    statFileGroup = fuseCtxGroupID ctx,
    statSpecialDeviceID = 0,
    statFileSize = 4096,
    statBlocks = 1,
    statAccessTime = 0,
    statModificationTime = 0,
    statStatusChangeTime = 0
}

fileStat ctx length = FileStat {
    statEntryType = SymbolicLink,
    statFileMode = foldr1 unionFileModes [
        ownerReadMode
    ],
    statLinkCount = 1,
    statFileOwner = fuseCtxUserID ctx,
    statFileGroup = fuseCtxGroupID ctx,
    statSpecialDeviceID = 0,
    statFileSize = length,
    statBlocks = 1,
    statAccessTime = 0,
    statModificationTime = 0,
    statStatusChangeTime = 0
}

helloGetFileStat :: FilePath -> IO (Either Errno FileStat)
helloGetFileStat path =
    case (parsePath path) of
        AttrList -> do
            ctx <- getFuseContext
            return $ Right $ dirStat ctx
        AttrVals _ -> do
            ctx <- getFuseContext
            return $ Right $ dirStat ctx
        View path _ -> do
            ctx <- getFuseContext
            tryFd <- try (openFd (repo ++ pathSep ++ path) ReadOnly Nothing defaultFileFlags) :: IO (Either SomeException Fd)
            result <- case tryFd of
                Left e -> return $ Left eNOENT
                Right fd -> do
                    status <- getFdStatus fd
                    case status of
                        _ | isRegularFile status -> return $ Right $ fileStat ctx 1
                          | isDirectory status -> return $ Right $ dirStat ctx
                          | otherwise -> return $ Left eNOENT
            return $ result

getFilterKeys :: [AnnexFile] -> [String]
getFilterKeys files =
    let
        fn AnnexFile{fields=fields} = fields
        listsOfFields = map fn files
        listsOfKeys = foldl (\a f -> a ++ HashMap.keys f) [] listsOfFields
        goodFields = filter (\e -> not $ isSuffixOf "lastchanged" e) listsOfKeys
    in
        nub goodFields

getFilterMap :: [AnnexFile] -> HashMap.HashMap String (HashSet.HashSet String)
getFilterMap files =
    let
        insertFieldsIntoMap map (name, values) = HashMap.insertWith (\new old -> HashSet.union new old) name (HashSet.fromList values) map
        processFile res AnnexFile{file=filename,fields=fields} =
            let
                valueFields = HashMap.filterWithKey (\k _ -> not $ isSuffixOf "lastchanged" k) fields
            in
                foldl insertFieldsIntoMap res (HashMap.toList valueFields)
    in
        foldl (\res f -> processFile res f) HashMap.empty files

filterFiles :: [AnnexFile] -> String -> [Condition] -> [AnnexFile]
filterFiles files (path) conditions =
    let
        matchPath AnnexFile{file=name} = isPrefixOf path name
        matchConditions AnnexFile{fields=fields} = all (\(ck, cv) -> any (\(k, vs) -> any (\v -> ck == k && cv == v) vs) (HashMap.toList fields)) conditions
    in
        filter (\f -> matchPath f && matchConditions f) files

helloFSOps :: (HashMap.HashMap String (HashSet.HashSet String)) -> [AnnexFile] -> FuseOperations HT
helloFSOps filterMap files = defaultFuseOps {
    fuseGetFileStat = helloGetFileStat,
    fuseReadSymbolicLink = my_readSymbolicLink,
    fuseOpenDirectory = helloOpenDirectory filterMap,
    fuseReadDirectory = helloReadDirectory filterMap files,
    fuseGetFileSystemStats = helloGetFileSystemStats
}

my_readSymbolicLink :: FilePath -> IO (Either Errno FilePath)
my_readSymbolicLink path =
    let
        request = parsePath path
    in
        return $ case request of
            View path conditions -> Right $ repo ++ pathSep ++ path
            _ -> Left $ eNOENT

helloOpenDirectory filterMap path =
    let
        request = parsePath path
    in
        return $ case request of
            AttrList -> eOK
            AttrVals attr -> eOK
            View path conditions ->
                case path of
                    _ -> eOK
                    --_ -> eNOENT

data EntityType = File | Folder deriving (Eq, Show)

getFilename :: String -> AnnexFile -> (String, EntityType)
getFilename path AnnexFile{file=name} =
    let
        prefix = if path == "" then "" else (path ++ pathSep)
        stripped = Maybe.fromJust (stripPrefix prefix name)
        baseName = takeWhile (\e -> e /= (head pathSep)) stripped
    in
        if baseName == stripped then
            (baseName, File)
        else
            (baseName, Folder)

helloReadDirectory :: (HashMap.HashMap String (HashSet.HashSet String)) -> [AnnexFile] -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
helloReadDirectory filterMap files path = do
    ctx <- getFuseContext
    return $ case (parsePath path) of
        AttrList ->
            Right $ map (\attr -> (attr, dirStat ctx)) ([allFolderName, ".", ".."] ++ (HashMap.keys filterMap))
        AttrVals attr ->
            case HashMap.lookup attr filterMap of
                Just values -> Right $ map (\value -> (value, dirStat ctx)) ([".", ".."] ++ HashSet.toList values)
                Nothing -> Left eNOENT
        View path conditions ->
            case path of
                _ -> Right $ map (\(a, t) -> (a, (case t of Folder -> dirStat ctx; File -> fileStat ctx 1))) ([(".", Folder), ("..", Folder)] ++ nub (map (getFilename path) (filterFiles files path conditions)))
                --_ -> Left eNOENT

helloOpen :: FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno HT)
helloOpen path mode flags = return (Left eNOENT)

helloRead :: FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
helloRead _ _ _ _ = return $ Left eNOENT

main :: IO ()
main = do
    a <- readCreateProcess ((shell "git annex metadata --json | jq '{ file: .file, fields: .fields}' | jq -s .") { cwd = Just repo }) ""
    case (Aeson.decode (fromString a) :: Maybe [AnnexFile]) of
        Just a -> fuseMain (helloFSOps (getFilterMap a) a)  (\e -> print e >> defaultExceptionHandler e)
