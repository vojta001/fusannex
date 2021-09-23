import Control.Exception (try)
import qualified Control.Exception as Exception (SomeException)

import qualified Data.List.Split as List.Split (splitOn)
import qualified Data.List as List
import Data.Text as Text (pack)
import qualified Data.ByteString.Char8 as ByteString.Char8
import qualified Data.ByteString.Lazy.UTF8 as ByteString.UTF8 (fromString, toString)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import qualified Data.Maybe as Maybe

import qualified System.Fuse as Fuse
import Foreign.C.Error (eOK, eNOENT, Errno)
import qualified System.Posix.Types as Posix.Types
import qualified System.Posix.Files as Posix.Files
import qualified System.Posix.IO as Posix.IO
import qualified System.Process as Process

import qualified Data.Aeson as Aeson

type Condition = (String, String)
data Request = AttrList | AttrVals String | View String [Condition] deriving (Show)

data AnnexFile = AnnexFile {
    file :: String,
    fields :: HashMap.HashMap String [String]
} deriving (Show)

instance Aeson.FromJSON AnnexFile where
 parseJSON (Aeson.Object v) =
    AnnexFile <$> v Aeson..: Text.pack "file"
           <*> v Aeson..: Text.pack "fields"

allFolderName = "all"
pathSep = "/"
repo = "/path/to/repo"

parsePath :: String -> Request
parsePath path =
    let
        parts = filter (\part -> part /= "") (List.Split.splitOn pathSep path)
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

getFileSystemStats :: String -> IO (Either Errno Fuse.FileSystemStats)
getFileSystemStats str =
    return $ Right $ Fuse.FileSystemStats {
        Fuse.fsStatBlockSize = 512,
        Fuse.fsStatBlockCount = 1,
        Fuse.fsStatBlocksFree = 1,
        Fuse.fsStatBlocksAvailable = 1,
        Fuse.fsStatFileCount = 5,
        Fuse.fsStatFilesFree = 10,
        Fuse.fsStatMaxNameLength = 255
    }

dirStat ctx = Fuse.FileStat {
    Fuse.statEntryType = Fuse.Directory,
    Fuse.statFileMode = foldr1 Fuse.unionFileModes [
        Posix.Files.ownerReadMode,
        Posix.Files.ownerExecuteMode
    ],
    Fuse.statLinkCount = 2,
    Fuse.statFileOwner = Fuse.fuseCtxUserID ctx,
    Fuse.statFileGroup = Fuse.fuseCtxGroupID ctx,
    Fuse.statSpecialDeviceID = 0,
    Fuse.statFileSize = 4096,
    Fuse.statBlocks = 1,
    Fuse.statAccessTime = 0,
    Fuse.statModificationTime = 0,
    Fuse.statStatusChangeTime = 0
}

fileStat ctx length = Fuse.FileStat {
    Fuse.statEntryType = Fuse.SymbolicLink,
    Fuse.statFileMode = foldr1 Fuse.unionFileModes [
        Posix.Files.ownerReadMode
    ],
    Fuse.statLinkCount = 1,
    Fuse.statFileOwner = Fuse.fuseCtxUserID ctx,
    Fuse.statFileGroup = Fuse.fuseCtxGroupID ctx,
    Fuse.statSpecialDeviceID = 0,
    Fuse.statFileSize = length,
    Fuse.statBlocks = 1,
    Fuse.statAccessTime = 0,
    Fuse.statModificationTime = 0,
    Fuse.statStatusChangeTime = 0
}

getFileStat :: FilePath -> IO (Either Errno Fuse.FileStat)
getFileStat path =
    case (parsePath path) of
        AttrList -> do
            ctx <- Fuse.getFuseContext
            return $ Right $ dirStat ctx
        AttrVals _ -> do
            ctx <- Fuse.getFuseContext
            return $ Right $ dirStat ctx
        View path _ -> do
            ctx <- Fuse.getFuseContext
            tryFd <- try (Posix.IO.openFd (repo ++ pathSep ++ path) Posix.IO.ReadOnly Nothing Posix.IO.defaultFileFlags) :: IO (Either Exception.SomeException Posix.Types.Fd)
            result <- case tryFd of
                Left e -> return $ Left eNOENT
                Right fd -> do
                    status <- Posix.Files.getFdStatus fd
                    case status of
                        _ | Posix.Files.isRegularFile status -> return $ Right $ fileStat ctx 1
                          | Posix.Files.isDirectory status -> return $ Right $ dirStat ctx
                          | otherwise -> return $ Left eNOENT
            return $ result

getFilterKeys :: [AnnexFile] -> [String]
getFilterKeys files =
    let
        fn AnnexFile{fields=fields} = fields
        listsOfFields = map fn files
        listsOfKeys = foldl (\a f -> a ++ HashMap.keys f) [] listsOfFields
        goodFields = filter (\e -> not $ List.isSuffixOf "lastchanged" e) listsOfKeys
    in
        List.nub goodFields

getFilterMap :: [AnnexFile] -> HashMap.HashMap String (HashSet.HashSet String)
getFilterMap files =
    let
        insertFieldsIntoMap map (name, values) = HashMap.insertWith (\new old -> HashSet.union new old) name (HashSet.fromList values) map
        processFile res AnnexFile{file=filename,fields=fields} =
            let
                valueFields = HashMap.filterWithKey (\k _ -> not $ List.isSuffixOf "lastchanged" k) fields
            in
                foldl insertFieldsIntoMap res (HashMap.toList valueFields)
    in
        foldl (\res f -> processFile res f) HashMap.empty files

filterFiles :: [AnnexFile] -> String -> [Condition] -> [AnnexFile]
filterFiles files (path) conditions =
    let
        matchPath AnnexFile{file=name} = List.isPrefixOf path name
        matchConditions AnnexFile{fields=fields} = all (\(ck, cv) -> any (\(k, vs) -> any (\v -> ck == k && cv == v) vs) (HashMap.toList fields)) conditions
    in
        filter (\f -> matchPath f && matchConditions f) files

fsOps :: (HashMap.HashMap String (HashSet.HashSet String)) -> [AnnexFile] -> Fuse.FuseOperations HT
fsOps filterMap files = Fuse.defaultFuseOps {
    Fuse.fuseGetFileStat = getFileStat,
    Fuse.fuseReadSymbolicLink = readSymbolicLink,
    Fuse.fuseOpenDirectory = openDirectory filterMap,
    Fuse.fuseReadDirectory = readDirectory filterMap files,
    Fuse.fuseGetFileSystemStats = getFileSystemStats
}

readSymbolicLink :: FilePath -> IO (Either Errno FilePath)
readSymbolicLink path =
    let
        request = parsePath path
    in
        return $ case request of
            View path conditions -> Right $ repo ++ pathSep ++ path
            _ -> Left $ eNOENT

openDirectory :: (HashMap.HashMap String (HashSet.HashSet String)) -> FilePath -> IO Errno
openDirectory filterMap path =
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

data EntityType = File | Directory deriving (Eq, Show)

getFilename :: String -> AnnexFile -> (String, EntityType)
getFilename path AnnexFile{file=name} =
    let
        prefix = if path == "" then "" else (path ++ pathSep)
        stripped = Maybe.fromJust (List.stripPrefix prefix name)
        baseName = takeWhile (\e -> e /= (head pathSep)) stripped
    in
        if baseName == stripped then
            (baseName, File)
        else
            (baseName, Directory)

readDirectory :: (HashMap.HashMap String (HashSet.HashSet String)) -> [AnnexFile] -> FilePath -> IO (Either Errno [(FilePath, Fuse.FileStat)])
readDirectory filterMap files path = do
    ctx <- Fuse.getFuseContext
    return $ case (parsePath path) of
        AttrList ->
            Right $ map (\attr -> (attr, dirStat ctx)) ([allFolderName, ".", ".."] ++ (HashMap.keys filterMap))
        AttrVals attr ->
            case HashMap.lookup attr filterMap of
                Just values -> Right $ map (\value -> (value, dirStat ctx)) ([".", ".."] ++ HashSet.toList values)
                Nothing -> Left eNOENT
        View path conditions ->
            case path of
                _ -> Right $ map (\(a, t) -> (a, (case t of Directory -> dirStat ctx; File -> fileStat ctx 1))) ([(".", Directory), ("..", Directory)] ++ List.nub (map (getFilename path) (filterFiles files path conditions)))
                --_ -> Left eNOENT

open :: FilePath -> Posix.IO.OpenMode -> Posix.IO.OpenFileFlags -> IO (Either Errno HT)
open path mode flags = return (Left eNOENT)

read :: FilePath -> HT -> Posix.Types.ByteCount -> Posix.Types.FileOffset -> IO (Either Errno ByteString.Char8.ByteString)
read _ _ _ _ = return $ Left eNOENT

main :: IO ()
main = do
    a <- Process.readCreateProcess ((Process.shell "git annex metadata --json | jq '{ file: .file, fields: .fields}' | jq -s .") { Process.cwd = Just repo }) ""
    case (Aeson.decode (ByteString.UTF8.fromString a) :: Maybe [AnnexFile]) of
        Just a -> Fuse.fuseMain (fsOps (getFilterMap a) a)  (\e -> print e >> Fuse.defaultExceptionHandler e)
