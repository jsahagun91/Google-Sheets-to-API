{ pkgs }: {
    deps = [
      pkgs.postgresql
      pkgs.glibcLocales
      pkgs.libiconv
    ];
}