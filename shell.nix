{ use_zsh ? "true" }:
let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/archive/c7f47036d3df2add644c46d712d14262b7d86c0c.tar.gz";
  pkgs = import nixpkgs { config = {}; overlays = []; };
in

pkgs.mkShell {
  packages = [
   pkgs.clojure
   pkgs.babashka
   pkgs.clj-kondo
   pkgs.cljfmt
   pkgs.jdk25
   pkgs.zsh
  ];

  shellHook = ''
    if [[ ${use_zsh} = true ]]
    then
      exec zsh
    fi
  '';
}
