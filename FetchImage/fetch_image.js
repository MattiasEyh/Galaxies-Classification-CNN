const dataForge = require('data-forge-fs');
const fs = require('fs');
const client = require('http');
const path = require('path');
const util = require('util');
const yargs = require('yargs');

const argv = yargs
    .options({
        'out': {
            alias: 'o',
            description: 'Path to put images',
            type: 'string',
            normalize: true
        },
        'imageOption': {
            alias: 'io',
            description: 'The option to use when downloading images',
            choices: ['G', 'O', 'I', 'B']
        },
        'step': {
            alias: 'st',
            description: 'Number of images to download simultaneously',
            type: 'number',
            default: 30
        },
        'skip': {
            alias: 'sk',
            description: 'Number of images to skip on download',
            type: 'number',
            default: 0
        }
    })
    .demandOption('out', 'Please provide a path to put downloaded images')
    .check((argv, options) => {
        if ( fs.existsSync(argv.out) && fs.lstatSync(argv.out).isDirectory() ) {
            return true            
        }
        throw new Error("The path must be an existing directory")
    })
  .alias('help', 'h').argv;

var BASE_URL = "http://skyserver.sdss.org/dr16/SkyServerWS/ImgCutout/getjpeg"
var DIRECTORY = argv.out
var WIDTH = argv.width
var HEIGHT = argv.height
var OPTIONS = argv.imageOption || ""
if ( Array.isArray(OPTIONS) ) OPTIONS = OPTIONS.reduce((r, c) => r + c)

async function downloadImage(row) {
    return new Promise((resolve) => {
        var ra = row["RA"]
        var dec = row["DEC"]
        var objId = row["OBJID"]
        var type = row["TYPE"]
        var imageDirectory = DIRECTORY + path.sep + type + path.sep;
        fs.existsSync(imageDirectory) || fs.mkdirSync(imageDirectory)
        let url = util.format("%s?ra=%s&dec=%s&opt=%s", BASE_URL, ra, dec, OPTIONS)
        client.get(url, (res) => {
            let file = fs.createWriteStream(imageDirectory + objId + ".jpeg")
            res.pipe(file)
            file.on("close", () => { resolve() })
        }).on("error", (e) => console.log(e))
    }, (error) => { console.log(error)})
}

async function downloadImages(df, step) {
    var nbProcessed = 0
    var size = df.count()
    while ( nbProcessed < size ) {
        let dfToProcess = df.skip(nbProcessed).take(step)
        let tabPromise = []
        dfToProcess.forEach( row => {
            tabPromise.push(downloadImage(row))
        })
        await Promise.all(tabPromise)
        nbProcessed += dfToProcess.count()
        console.log("%d/%d", nbProcessed, size)
    }
}

let toSkip = argv.skip
var step = argv.step
var df = dataForge.readFileSync("../Datas/dataWithPreProcess.csv")
    .parseCSV()
    .skip(toSkip)

console.log("Downloading images...")
downloadImages(df, step)