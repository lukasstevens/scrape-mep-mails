let
    pkgs = import <nixpkgs> {};
in
  pkgs.stdenv.mkDerivation {
    name = "mep-mail";

    buildInputs = [ pkgs.python37 pkgs.python37Packages.requests pkgs.python37Packages.beautifulsoup4 pkgs.python37Packages.aiohttp ];
  }
