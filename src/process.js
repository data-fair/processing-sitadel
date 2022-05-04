const fs = require('fs-extra')
const util = require('util')
const pump = util.promisify(require('pump'))
const csv = require('csv')
const csvSync = require('csv/sync')
const config = require('config')
const stream = require('stream')
const FormData = require('form-data')

let header = false

function measure (lat1, lon1, lat2, lon2) { // generally used geo measurement function
  // console.log(lat1, lon1, lat2, lon2)
  const R = 6371 // Radius of earth in KM
  const dLat = lat2 * Math.PI / 180 - lat1 * Math.PI / 180
  const dLon = lon2 * Math.PI / 180 - lon1 * Math.PI / 180
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  const d = R * c
  return d * 1000 // meters
}

async function geocode (arr, axios, log) {
  // create a deep copy of the array
  // arr = JSON.parse(JSON.stringify(arr))
  log.info(`Géocodage de ${arr.length} items`, '')

  let csvString = Object.keys(arr[0]).join(',') + '\n'
  csvString += csvSync.stringify(arr)
  // console.log(csvString)

  const form = new FormData()
  form.append('data', csvString, 'filename')
  form.append('columns', 'ADR_NUM_TER')
  form.append('columns', 'ADR_LIBVOIE_TER')
  form.append('columns', 'ADR_LIEUDIT_TER')
  form.append('columns', 'ADR_LOCALITE_TER')
  form.append('citycode', 'COMM')
  form.append('result_columns', 'result_type')
  form.append('result_columns', 'latitude')
  form.append('result_columns', 'longitude')

  const response = await axios.post(
    'https://api-adresse.data.gouv.fr/search/csv/',
    form,
    {
      headers: {
        ...form.getHeaders()
      }
    }
  )

  if (!header) {
    header = true
    return response.data
  }
  // remove the header
  return response.data.substring(response.data.indexOf('\n') + 1)
}

async function getParcel (input, stats, axios, log) {
  stats.currDP = input.DEP
  if (stats.currDP !== stats.lastDP) {
    log.info(`Traitement du departement ${stats.currDP}`)
    stats.lastDP = stats.currDP
  }
  if (input.num_cadastre1.trim().length) {
    const codeParcelle = `${input.COMM}[0-9]{3}[A-Z]{2}${input.num_cadastre1.padStart(4, '0')}`
    const param = {
      headers: {
        'x-apiKey': config.dataFairAPIKey
      },
      params: {
        qs: `code:/${codeParcelle}/`,
        size: 100
      },
      timeout: 3000
    }
    let codeParcelleTotal

    try {
      codeParcelleTotal = (await axios.get('https://staging-koumoul.com/data-fair/api/v1/datasets/cadastre-parcelles-coords/lines', param)).data
    } catch (e) {
      log.error('getParcel error', e.message)
    }

    let stoElem
    input.geocoding = {}
    input.geocoding.lat = input.latitude
    input.geocoding.lon = input.longitude
    input.geocoding.result_type = input.result_type

    delete input.latitude
    delete input.longitude
    delete input.result_type

    // console.log(codeParcelleTotal.results)
    if (codeParcelleTotal !== undefined) {
      const matches = codeParcelleTotal.results.filter(s => s.code.substr(8, 2) === input.sec_cadastre1)
      if (matches.length > 0) {
        if (matches.length === 1) {
          stoElem = matches[0]
          stats.sur++
          input.parcel_confidence = 100 + ' %'
          input.latitude = stoElem.coord.split(',')[1]
          input.longitude = stoElem.coord.split(',')[0]
          input.parcelle = stoElem.code
        } else {
          let min, secondD
          for (const elem of matches) {
            // console.log(elem.coord.split(',')[1], elem.coord.split(',')[0])
            const dist = measure(parseFloat(elem.coord.split(',')[1]), parseFloat(elem.coord.split(',')[0]), input.geocoding.lat, input.geocoding.lon)
            if (!min || dist < min) {
              min = dist
              stoElem = elem
              secondD = dist
            } else if (!secondD || dist < secondD) {
              secondD = dist
            }
          }
          const percent = Math.round(100 * secondD / (min + secondD))
          input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
          input.latitude = stoElem.coord.split(',')[1]
          input.longitude = stoElem.coord.split(',')[0]
          input.parcelle = stoElem.code
          stats.geocode++
        }
      } else {
        if (codeParcelleTotal.results.length === 1) {
          stoElem = codeParcelleTotal.results[0]

          // console.log('On est sûr du résultat. Une seule parcelle disponible')
          // console.log(stoElem.code, stoElem.coord, ':', input.geocoding.lat, input.geocoding.lon)
          stats.sur++
          input.parcel_confidence = 100 + ' %'
          input.latitude = stoElem.coord.split(',')[1]
          input.longitude = stoElem.coord.split(',')[0]
          input.parcelle = stoElem.code
        } else if (codeParcelleTotal.results.length > 0 && (input.geocoding.result_type === 'housenumber' || input.geocoding.result_type === 'street')) {
          let min, secondD
          for (const elem of codeParcelleTotal.results) {
            // console.log(elem.coord.split(',')[1], elem.coord.split(',')[0])
            const dist = measure(parseFloat(elem.coord.split(',')[1]), parseFloat(elem.coord.split(',')[0]), input.geocoding.lat, input.geocoding.lon)
            if (!min || dist < min) {
              min = dist
              stoElem = elem
              secondD = dist
            } else if (!secondD || dist < secondD) {
              secondD = dist
            }
          }
          const percent = Math.round(100 * secondD / (min + secondD))
          input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
          input.latitude = stoElem.coord.split(',')[1]
          input.longitude = stoElem.coord.split(',')[0]
          input.parcelle = stoElem.code
          // console.log('On prend le plus proche parce que la précision du geocoder est pas mal')
          // console.log(min, stoElem.code, stoElem.coord, ':', input.geocoding.lat, input.geocoding.lon, Math.round(100 * secondD / (min + secondD)))
          stats.geocode++
        } else if (codeParcelleTotal.results.length) {
          let stoElem
          if (input.geocoding.lat === undefined) {
            stoElem = codeParcelleTotal.results[0]
            const percent3 = Math.round(100 / codeParcelleTotal.results.length)
            input.parcel_confidence = (percent3 < 25 ? '<25' : (percent3 + '').padStart(3, '0')) + ' %'.padStart(2, '0')
          } else {
            let min, secondD
            for (const elem of codeParcelleTotal.results) {
              // console.log(elem.coord.split(',')[1], elem.coord.split(',')[0])
              const dist = measure(parseFloat(elem.coord.split(',')[1]), parseFloat(elem.coord.split(',')[0]), input.geocoding.lat, input.geocoding.lon)
              if (!min || dist < min) {
                secondD = min
                min = dist
                stoElem = elem
              } else if (!secondD || dist < secondD) {
                secondD = dist
              }
            }
            const percent3 = Math.round(100 * secondD / (min + secondD))
            // console.log(percent3)
            input.parcel_confidence = (percent3 < 25 ? '<25' : (percent3 + '').padStart(3, '0')) + ' %'.padStart(2, '0')
          }
          input.latitude = stoElem.coord.split(',')[1]
          input.longitude = stoElem.coord.split(',')[0]
          input.parcelle = stoElem.code
          // On a une précision mauvaise, on chope juste le premier résultat des ${codeParcelleTotal.results.length} disponibles
          stats.premier++
        } else {
          input.latitude = undefined
          input.longitude = undefined
          input.parcel_confidence = undefined
          stats.erreur++
        }
      }
    } else {
      stats.erreur++
    }
    delete input.geocoding
  } else {
    // num_cadastre1 null
    stats.erreur++
  }
  return input
}

const extend = async (processingConfig, axios, log) => {
  for (const filename of ['t.csv']) {
    console.log(filename)

    const stats = {
      sur: 0,
      premier: 0,
      geocode: 0,
      erreur: 0,
      currDP: undefined,
      lastDP: undefined
    }

    const tab = []
    await pump(
      fs.createReadStream(filename),
      csv.parse({ columns: true, delimiter: ';' }),
      new stream.Transform({
        objectMode: true,
        transform: async (obj, _, next) => {
          tab.push(obj)
          if (tab.length >= 1000) {
            const result = await geocode(tab, axios, log)
            tab.length = 0
            return next(null, result)
          }
          return next()
        },
        flush: async (callback) => {
          if (tab.length > 0) {
            const result = await geocode(tab, axios, log)
            callback(null, result)
          }
        }
      }),
      csv.parse({ columns: true, delimiter: ',' }),
      new stream.Transform({
        objectMode: true,
        transform: async (obj, _, next) => {
          next(null, await getParcel(obj, stats, axios, log))
        }
      }),
      csv.stringify({ header: true, quoted_string: true }),
      fs.createWriteStream(processingConfig.datasetIdPrefix + '-' + filename)
    )
    const sum = stats.sur + stats.geocode + stats.premier + stats.erreur
    console.log(`Sûr : ${Math.round(stats.sur * 100 / sum)}%, Géocodé : ${Math.round(stats.geocode * 100 / sum)}%, Premier : ${Math.round(stats.premier * 100 / sum)}%, Non défini : ${Math.round(stats.erreur * 100 / sum)}%, Total : ${sum}`)
  }
}

module.exports = async (processingConfig, axios, log) => {
  await extend(processingConfig, axios, log)
}
