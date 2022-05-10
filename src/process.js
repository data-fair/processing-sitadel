const fs = require('fs-extra')
const util = require('util')
const pump = util.promisify(require('pump'))
const csv = require('csv')
const csvSync = require('csv/sync')
const config = require('config')
const stream = require('stream')
const FormData = require('form-data')
const mergeSortStream = require('merge-sort-stream')
const filter = require('stream-filter')

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
  log.info(`Géocodage de ${arr.length} items`, '')

  // add the header for the request
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

async function getParcel (array, stats, axios, log) {
  let stringRequest = ''
  let a = [...new Set(array.map(elem => elem.num_cadastre1.padStart(4, '0')))]
  if (a.length > 1400) {
    log.error('getParcel, trop de parcelles pour l\'URL')
    for (const i of array) {
      delete i.result_type
      i.latitude = undefined
      i.longitude = undefined
      i.parcel_confidence = undefined
      i.parcelle = undefined
    }
    return csvSync.stringify(array)
  }
  const ecart = 198
  do {
    stringRequest += `/.*(${a.slice(0, ecart).join('|')})/`
    a = a.slice(ecart, a.length + 1)
    if (a.length > 0) stringRequest += ' OR '
  } while (a.length > ecart)
  if (a.length > 0) stringRequest += `/.*(${a.slice(0, ecart).join('|')})/`

  const param = {
    headers: {
      'x-apiKey': config.dataFairAPIKey
    },
    params: {
      qs: `code:(/${array[0].COMM}.{9}/ AND (${stringRequest}))`,
      size: 0
    }
  }
  // console.log(param.params.qs)

  const startTime = new Date().getTime()
  const nbParcels = (await axios.get('https://staging-koumoul.com/data-fair/api/v1/datasets/cadastre-parcelles-coords/lines', param)).data.total

  const end = new Date().getTime()
  log.info(`Temps requête nbParcels : ${end - startTime} ms`)
  // console.log(nbParcels)

  param.params.size = nbParcels
  if (param.params.size > 10000) param.params.size = 10000

  const start2 = new Date().getTime()
  let parcels = (await axios.get('https://staging-koumoul.com/data-fair/api/v1/datasets/cadastre-parcelles-coords/lines', param)).data

  const commParcels = parcels.results
  let next = parcels.next

  for (let tour = 1; tour < Math.ceil(nbParcels / 10000); tour++) {
    parcels = (await axios.get(next, { headers: { 'x-apiKey': config.dataFairAPIKey } })).data
    commParcels.push(...parcels.results)
    next = parcels.next
  }

  const end2 = new Date().getTime()
  log.info(`Temps requête 2 : ${end2 - start2} ms`)
  stats.moyReq += end2 - start2
  stats.sum += 1

  const ret = []
  const startTraitement = new Date().getTime()
  for (const input of array) {
    input.geocoding = {}
    input.geocoding.lat = input.latitude
    input.geocoding.lon = input.longitude
    input.geocoding.result_type = input.result_type

    delete input.latitude
    delete input.longitude
    delete input.result_type

    if (input.num_cadastre1.trim().length) {
      const codeParcelleTotal = commParcels.filter(elem => elem.code.match(new RegExp(`${array[0].COMM}.....${input.num_cadastre1.padStart(4, '0')}`)))

      let stoElem

      if (codeParcelleTotal !== undefined) {
        const matches = codeParcelleTotal.filter(s => s.code.substr(8, 2) === input.sec_cadastre1)
        if (matches.length > 0) {
          if (matches.length === 1) {
            stoElem = matches[0]
            stats.sur++
            input.latitude = stoElem.coord.split(',')[1]
            input.longitude = stoElem.coord.split(',')[0]
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem.code
          } else {
            let min, secondD
            for (const elem of matches) {
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
            input.latitude = stoElem.coord.split(',')[1]
            input.longitude = stoElem.coord.split(',')[0]
            input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            input.parcelle = stoElem.code
            stats.geocode++
          }
        } else {
          if (codeParcelleTotal.length === 1) {
            stoElem = codeParcelleTotal[0]

            // On est sûr du résultat. Une seule parcelle disponible
            stats.sur++
            input.latitude = stoElem.coord.split(',')[1]
            input.longitude = stoElem.coord.split(',')[0]
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem.code
          } else if (codeParcelleTotal.length > 0 && (input.geocoding.result_type === 'housenumber' || input.geocoding.result_type === 'street')) {
            let min, secondD
            for (const elem of codeParcelleTotal) {
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
            input.latitude = stoElem.coord.split(',')[1]
            input.longitude = stoElem.coord.split(',')[0]
            input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            input.parcelle = stoElem.code
            // On prend le plus proche parce que la précision du geocoder est correcte
            stats.geocode++
          } else if (codeParcelleTotal.length) {
            let stoElem, confidence
            if (input.geocoding.lat === undefined) {
              stoElem = codeParcelleTotal[0]
              const percent3 = Math.round(100 / codeParcelleTotal.length)
              confidence = (percent3 < 25 ? '<25' : (percent3 + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            } else {
              let min, secondD
              for (const elem of codeParcelleTotal) {
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
              confidence = (percent3 < 25 ? '<25' : (percent3 + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            }
            input.latitude = stoElem.coord.split(',')[1]
            input.longitude = stoElem.coord.split(',')[0]
            input.parcel_confidence = confidence
            input.parcelle = stoElem.code
            stats.premier++
          } else {
            input.latitude = undefined
            input.longitude = undefined
            input.parcel_confidence = undefined
            input.parcelle = undefined
            stats.erreur++
          }
        }
      } else {
        input.latitude = undefined
        input.longitude = undefined
        input.parcel_confidence = undefined
        input.parcelle = undefined
        stats.erreur++
      }
    } else {
      // num_cadastre1 null
      input.latitude = undefined
      input.longitude = undefined
      input.parcel_confidence = undefined
      input.parcelle = undefined
      stats.erreur++
    }
    delete input.geocoding
    ret.push(input)
  }
  const endTraitement = new Date().getTime()
  log.info(`Temps traitement : ${endTraitement - startTraitement} ms`)

  if (!stats.header) {
    stats.header = true
    return csvSync.stringify(ret, { header: true })
  }
  return csvSync.stringify(ret)
}

function compare (a, b) {
  // define wich num to use to filter
  const type = Object.keys(a).filter(e => e.match(/Num_.*/g))
  a = a[type]
  b = b[type]
  return (b).localeCompare(a)
}

async function fusion (a, b, option, log) {
  if (b === undefined) return fs.createReadStream(a, { objectMode: true }).pipe(csv.parse({ columns: true, delimiter: ';' })).pipe(filter(function (data) { return option.includes(parseInt(data.DEP)) }, { objectMode: true }))

  log.step(`Fusion de ${a} et ${b}. Option : ${option}`)

  const h1 = new Promise(function (resolve) {
    fs.createReadStream(a, { objectMode: true }).on('data', function (data) { resolve(data.toString().split('\n')[0]) })
  })

  const h2 = new Promise(function (resolve) {
    fs.createReadStream(b, { objectMode: true }).on('data', function (data) { resolve(data.toString().split('\n')[0]) })
  })

  if (await h1 !== await h2) {
    log.error('Erreur : Les deux CSVs ne possèdent pas la même en-tête')
    throw new Error('Erreur : Les deux CSVs ne possèdent pas la même en-tête')
  } else {
    log.info('Fichiers compatibles')
  }

  const f1 =
     fs.createReadStream(a, { objectMode: true })
       .pipe(csv.parse({ columns: true, delimiter: ';' }))
       .pipe(filter(function (data) { return option.includes(parseInt(data.DEP)) }, { objectMode: true }))

  const f2 =
    fs.createReadStream(b, { objectMode: true })
      .pipe(csv.parse({ columns: true, delimiter: ';' }))
      .pipe(filter(function (data) { return option.includes(parseInt(data.DEP)) }, { objectMode: true }))

  return mergeSortStream(f1, f2, compare)
}

const extend = async (processingConfig, axios, log) => {
  const stats = {
    sur: 0,
    premier: 0,
    geocode: 0,
    erreur: 0,
    header: false,
    moyReq: 0,
    sum: 0
  }
  let currComm
  const batchComm = []

  const tab = []

  let dir = await fs.readdir('.')
  dir = dir.filter(file => file.endsWith('.csv') && file.includes(processingConfig.processFile) && !file.startsWith(processingConfig.datasetIdPrefix))
  const file1 = dir[0]
  const file2 = dir[1]
  await pump(
    await fusion(file1, file2, processingConfig.departements, log),
    new stream.Transform({
      objectMode: true,
      transform: async (obj, _, next) => {
        tab.push(obj)
        if (tab.length >= 10000) {
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
        if (obj.COMM === currComm || currComm === undefined) {
          batchComm.push(obj)
          currComm = obj.COMM
        } else if (batchComm.length > 0) {
          log.step(`Traitement de ${batchComm.length} parcelle(s) dans la commune ${currComm}`, '')
          const result = await getParcel(batchComm, stats, axios, log)
          currComm = obj.COMM
          batchComm.length = 0
          batchComm.push(obj)
          return next(null, result)
        }
        return next()
      },
      flush: async (callback) => {
        if (batchComm.length > 0) {
          log.step(`Traitement de ${batchComm.length} parcelle(s) dans la commune ${currComm}`, '')
          const result = await getParcel(batchComm, stats, axios, log)
          callback(null, result)
        }
      }
    }),
    csv.parse({ columns: true, delimiter: ',' }),
    csv.stringify({ header: true, quoted_string: true }),
    fs.createWriteStream(processingConfig.datasetIdPrefix + '-' + processingConfig.processFile + '.csv')
  )
  const sum = stats.sur + stats.geocode + stats.premier + stats.erreur
  log.step('Fin du traitement')
  log.info(`Sûr : ${Math.round(stats.sur * 100 / sum)}%, Géocodé : ${Math.round(stats.geocode * 100 / sum)}%, Géododé peu précis : ${Math.round(stats.premier * 100 / sum)}%, Non défini : ${Math.round(stats.erreur * 100 / sum)}%, Total : ${sum}`)
  log.info(`Moy requête 2 : ${Math.round(stats.moyReq / stats.sum)} ms`, '')
}

module.exports = async (processingConfig, axios, log) => {
  await extend(processingConfig, axios, log)
}
