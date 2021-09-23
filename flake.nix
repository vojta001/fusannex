{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/master";

  outputs = { self, nixpkgs }:
  let
    pkgs = import nixpkgs { system = "x86_64-linux"; };
  in
    {
      devShell.x86_64-linux =
      let
        libs = p: [ p.split p.HFuse p.aeson p.utf8-string ];
      in
        pkgs.mkShell {
          nativeBuildInputs = with pkgs; [ (haskellPackages.ghcWithPackages libs) ];
        };
    };
}
