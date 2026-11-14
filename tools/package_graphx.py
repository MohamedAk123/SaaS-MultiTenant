"""The Spark image has a JRE; package Scala class files without requiring jar/JDK."""
from pathlib import Path
from zipfile import ZipFile,ZIP_DEFLATED
base=Path('/workspace/artifacts/graphx-classes')
with ZipFile('/workspace/artifacts/graphx.jar','w',ZIP_DEFLATED) as output:
    for path in sorted(base.rglob('*.class')):
        output.write(path,path.relative_to(base).as_posix())
