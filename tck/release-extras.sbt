version in ThisBuild := (version in ThisBuild).value + "-" + {new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)} + "-SNAPSHOT"
