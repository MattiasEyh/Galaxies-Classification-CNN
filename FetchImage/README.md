# Nodejs script to download images from sdss API.

In order to use this script you should have an existing directory to provide with the --out option.  
Other options are available like --imageOption (--io) to use image options provided by the sdss API (G for GRID, O for OUTLINE, I for INVERT IMAGE, B for BOUNDING BOX).  
If you want to use multiple options you need to write the same number of --io as options needed : --io G --io O. (Yargs package).

example :
```ShellSession 
node fetch_image.js --o yourpath --io I --io O --w 512 --h 512 --skip 300 --step 200
```

Warning : SDSS may likely stop you from downloading if you send too many requests simultaneously.