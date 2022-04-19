const fs = require('fs-extra')
const path = require('path')
const zlib = require('zlib')
const util = require('util')
const JSONStream = require('JSONStream')
const pump = util.promisify(require('pump'))
const { Writable } = require('stream')
const { fetch } = require('./files')

const load = async (dep, commune) => {
  let parcellesFile
  const prefixes = {}
  try {
    parcellesFile = await fetch(`https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/${dep}/cadastre-${dep}-parcelles.json.gz`, 'downloads', undefined, false)
  } catch (err) {
    console.error('Missing parcelles file', err)
    return
  }
  console.log('got file')
  await pump(
    fs.createReadStream(parcellesFile),
    zlib.createUnzip(),
    JSONStream.parse('features.*'),
    new Writable({
      async write (f, encoding, callback) {
        prefixes[f.properties.commune] = prefixes[f.properties.commune] || {}
        const pref = f.properties.prefixe + '-' + f.properties.section
        prefixes[f.properties.commune][pref] = (prefixes[f.properties.commune][pref] || 0) + 1
        callback()
      },
      objectMode: true
    })
  )
  // console.log(prefixes)
  fs.writeFileSync(path.join('prefixes', dep + '.json'), JSON.stringify(prefixes))
  // fs.writeFileSync(path.join('parcelsCoords', dep + '.json'), JSON.stringify(dict))
}
const deps = [
  '75'
  // '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2A', '2B',
  // '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
  // '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
  // '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
  // '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '971', '972', '973', '974', '976'
]
module.exports = async () => {
  await fs.ensureDir('prefixes')
  // await fs.ensureDir('parcelsCoords')
  for (const dep of deps) {
    await load(dep)
  }
}
