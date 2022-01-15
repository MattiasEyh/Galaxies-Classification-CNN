const dataForge = require('data-forge-fs');
const fs = require('fs');
const client = require('https');
const util = require('util');

var BASE_URL = "https://skyserver.sdss.org/dr16/SkyServerWS/ImgCutout/getjpeg"
var WIDTH = 512
var HEIGHT = 512
var BASE_FOLDER = "../Datas/Images/"

async function downloadImage(objId, ra, dec) {
    return new Promise((resolve) => {
        url = util.format("%s?ra=%s&dec=%s&width=%s&height=%s", BASE_URL, ra, dec, WIDTH, HEIGHT)
        client.get(url, (res) => {
            let file = fs.createWriteStream(BASE_FOLDER + objId + ".jpeg")
            res.pipe(file)
            file.on("close", () => { resolve() })
        }).on("error", (e) => console.log(e))
    }, (error) => { console.log(error)})
}

async function downloadImages(df, step) {
    var nbProcessed = 0
    var size = df.toRows().length
    while ( nbProcessed < size ) {
        let dfToProcess = df.skip(nbProcessed).take(step)
        let tabPromise = []
        dfToProcess.forEach( row => {
            let objId = row["OBJID"]
            let ra = row["RA"]
            let dec = row["DEC"]
            tabPromise.push(downloadImage(objId, ra, dec))
        })
        await Promise.all(tabPromise)
        nbProcessed += step
        console.log(util.format("%d/%d", nbProcessed, size))
    }
}

var toSkip = 0
var step = 300
var df = dataForge.readFileSync("../Datas/dataWithPreProcess.csv")
    .parseCSV()
    .subset(["OBJID", "RA", "DEC"])
    .skip(toSkip)


if (!fs.existsSync(BASE_FOLDER)){
    fs.mkdirSync(BASE_FOLDER);
}
downloadImages(df, step)