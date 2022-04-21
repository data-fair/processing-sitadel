const fs = require('fs-extra')
const path = require('path')
const zlib = require('zlib')
const util = require('util')
const JSONStream = require('JSONStream')
const pump = util.promisify(require('pump'))
const { Writable } = require('stream')
const { fetch } = require('./files')
const axios = require('axios')
const turf = require('@turf/turf')

const truncateDecimals = function (number) {
  return Math[number < 0 ? 'ceil' : 'floor'](number)
}

const load = async (dep, log) => {
  let parcellesFile
  const prefixes = {}
  const parcels = {}
  let added = 0
  let updated = 0
  const url = `https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/${dep}/cadastre-${dep}-parcelles.json.gz`
  const overwrite = false
  try {
    parcellesFile = await fetch(url)
  } catch (err) {
    console.error('Missing parcelles file', err)
    return
  }
  console.log('got file')

  const parsedPath = path.parse(new URL(url).pathname)
  const file = `${process.cwd()}/downloads/${parsedPath.name + parsedPath.ext}`
  const lastUpdateServer = Date.parse((await axios.head(url)).headers['last-modified'])
  const lastUpdateLocal = Date.parse((fs.statSync(file)).birthtime)

  // check if the local file is up to date to avoid creating the same version
  if (lastUpdateServer > lastUpdateLocal || !(await fs.pathExists(path.join('prefixes', dep + '.json'))) || !(await fs.pathExists(path.join('parcelsCoords', dep + '.json'))) || overwrite) {
    log.step(`Parsing parcels of departement ${dep}`)
    await pump(
      fs.createReadStream(parcellesFile),
      zlib.createUnzip(),
      JSONStream.parse('features.*'),
      new Writable({
        async write (f, encoding, callback) {
          prefixes[f.properties.commune] = prefixes[f.properties.commune] || {}
          const pref = f.properties.prefixe + '-' + f.properties.section
          prefixes[f.properties.commune][pref] = (prefixes[f.properties.commune][pref] || 0) + 1

          const centroid = turf.centroid(f)
          if (centroid) {
            if (parcels[f.properties.id]) updated++
            else added++
            parcels[f.properties.id] = centroid.geometry.coordinates.map(c => truncateDecimals(c * 1000000) / 1000000)
          }
          callback()
        },
        objectMode: true
      })
    )
    console.log('added', added, 'updated', updated)
    fs.writeFileSync(path.join('prefixes', dep + '.json'), JSON.stringify(prefixes))
    fs.writeFileSync(path.join('parcelsCoords', dep + '.json'), JSON.stringify(parcels))
  } else {
    log.info(`${file} already up to date. Last server update was : ${new Date(lastUpdateServer).toLocaleString()}`, '')
  }
}
const deps = [
  '01', '02'
  // '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2A', '2B',
  // '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
  // '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
  // '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
  // '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '971', '972', '973', '974', '976'
]
module.exports = async (log) => {
  await fs.ensureDir('prefixes')
  await fs.ensureDir('parcelsCoords')

  for (const dep of deps) {
    await load(dep, log)
  }
  log.info(`Loading of ${deps.length} departements done`, '')
}
