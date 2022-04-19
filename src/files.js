const fs = require('fs-extra')
const path = require('path')
const pump = require('util').promisify(require('pump'))
const JSONStream = require('JSONStream')
const { Transform } = require('stream')
const zlib = require('zlib')
const exec = require('./exec')

exports.fetchAndExtract = async (url, unzipDir, overwrite = true) => {
  const parsedPath = path.parse(new URL(url).pathname)
  const archiveFile = `downloads/${parsedPath.name}${parsedPath.ext}`
  const file = `downloads/${parsedPath.name}`
  // remove uncompressed result
  if (await fs.pathExists(file)) {
    if (overwrite) {
      await fs.remove(file)
    } else {
      console.log(`${file} already exists`)
      return file
    }
  }

  console.log(`download ${url}`)
  const ext = url.split('.').pop()
  // -N is for timestamping, so not re-fetching previous versions of the file
  // add --server-response occasionaly to debug output
  await exec(`wget -N -nv --no-check-certificate --retry-connrefused --waitretry=60 --timeout=120 -t 20 "${url}"`, process.cwd() + '/downloads/')
  console.log(`extract ${archiveFile} -> ${file}`)
  if (unzipDir) {
    await fs.ensureDir(file)
    await exec(`unzip ${archiveFile} -d ${file}`)
  } else if (ext === 'xz') {
    await fs.ensureDir(file)
    await exec(`tar -xJf ${archiveFile} -C ${file}`)
  } else if (ext === '001' || ext === '7z') {
    await exec(`7z x -bb0 -r ${archiveFile} -o${file}`)
  } else {
    await exec(`gunzip ${archiveFile}`)
  }
  return file
}

exports.fetch = async (url, dir = 'downloads', fileName) => {
  await fs.ensureDir(`${process.cwd()}/${dir}`)
  const parsedPath = path.parse(new URL(url).pathname)
  const file = `${process.cwd()}/${dir}/${fileName || parsedPath.name + parsedPath.ext}`

  console.log(`download ${url} -> ${file}`)
  // -N is for timestamping, so not re-fetching previous versions of the file
  // add --server-response occasionaly to debug output
  let cmd = `wget -N -nv --no-check-certificate --retry-connrefused --waitretry=60 --timeout=120 -t 20 "${url}"`
  if (fileName) cmd += ` -O ${fileName}`
  await exec(cmd, `${process.cwd()}/${dir}/`)
  return file
}

exports.extendGeojson = async (file, fn) => {
  console.log(`Extend geojson properties from ${file}`)

  if (await fs.pathExists(file + '.extended')) await fs.remove(file + '.extended')
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(file + '.extended')

  const inputStreams = [fs.createReadStream(file)]
  if (file.endsWith('.gz')) {
    inputStreams.push(zlib.createUnzip())
  }
  await pump(
    ...inputStreams,
    JSONStream.parse('features.*'),
    new Transform({
      transform (f, encoding, callback) {
        fn(f)
        callback(null, f)
      },
      objectMode: true
    }),
    JSONStream.stringify('{"type": "FeatureCollection", "features": [\n', ',\n', ']}'),
    fs.createWriteStream(file + '.extended')
  )
  return file + '.extended'
}
