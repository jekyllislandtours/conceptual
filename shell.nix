{ use_zsh ? "true" }:
let
  # as of 28 Oct 2025. See: https://status.nixos.org
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/archive/01f116e4df6a15f4ccdffb1bcd41096869fb385c.tar.gz";
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
