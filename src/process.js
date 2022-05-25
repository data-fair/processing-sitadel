const fs = require('fs-extra')
const util = require('util')
const pump = util.promisify(require('pump'))
const csv = require('csv')
const csvSync = require('csv/sync')
const stream = require('stream')
const FormData = require('form-data')
const mergeSortStream = require('merge-sort-stream')
const filter = require('stream-filter')
const path = require('path')

let header = false

function measure (lat1, lon1, lat2, lon2) { // generally used geo measurement function
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
  await log.info(`Géocodage de ${arr.length} éléments`, '')
  const start = new Date().getTime()
  // add the header for the request
  let csvString = Object.keys(arr[0]).join(',') + '\n'
  csvString += csvSync.stringify(arr)

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
  const duration = new Date().getTime() - start
  await log.info(`Géocodage réalisé en ${duration.toLocaleString('fr')} ms.`, '')

  if (!header) {
    header = true
    return response.data
  }
  // remove the header
  return response.data.substring(response.data.indexOf('\n') + 1)
}

async function getParcel (array, globalStats, pluginConfig, processingConfig, axios, log) {
  let stringRequest = ''
  let arrayUniNum = [...new Set(array.map(elem => elem.num_cadastre1.replace(/\D/g, '').padStart(4, '0')))]

  const ecart = 197
  do {
    stringRequest += `/${array[0].COMM}.{5}(${arrayUniNum.slice(0, ecart).join('|')})/`
    arrayUniNum = arrayUniNum.slice(ecart, arrayUniNum.length + 1)
  } while (arrayUniNum.length > ecart)
  if (arrayUniNum.length > 0) stringRequest += `/${array[0].COMM}.{5}(${arrayUniNum.slice(0, ecart).join('|')})/`

  const params = {
    size: 10000
  }

  // data is the array containing all of the results of requests
  const commParcels = []

  const start = new Date().getTime()

  // To avoid URL overflow, break
  const breakRequestAt = pluginConfig.urlLimit
  if (parseInt((stringRequest.length / breakRequestAt + 1)) > 1) {
    await log.info(`Besoin de ${parseInt((stringRequest.length / breakRequestAt + 1))} requêtes pour couvrir l'ensemble des parcelles de ${array[0].COMM}.`)
  }
  while (stringRequest.length > breakRequestAt) {
    // get the closest line delimiter to slice the string well
    let firstIndex
    const indexslash = stringRequest.indexOf('/') !== -1 ? stringRequest.indexOf('/') : Infinity
    const indexpar = stringRequest.indexOf('(') !== -1 ? stringRequest.indexOf('(') : Infinity
    const indexbar = stringRequest.indexOf('|') !== -1 ? stringRequest.indexOf('|') : Infinity

    if (indexslash < indexpar && indexslash < indexbar) firstIndex = indexslash
    if (indexpar < indexslash && indexpar < indexbar) firstIndex = indexpar
    if (indexbar < indexslash && indexbar < indexpar) firstIndex = indexbar

    let decal = 1
    if (stringRequest[firstIndex + 1] === '/') decal = 2
    let tmpString = stringRequest.slice(firstIndex + decal, breakRequestAt)

    const lastValue = tmpString.lastIndexOf('|') > tmpString.lastIndexOf(')') ? tmpString.lastIndexOf('|') : tmpString.lastIndexOf(')')

    tmpString = tmpString.substring(0, lastValue)
    if (tmpString.startsWith(array[0].COMM)) tmpString = tmpString.substring(tmpString.indexOf('(') + 1, lastValue)
    params.qs = `code:(/${array[0].COMM}.{5}(${tmpString})/)`

    // process the requests and add the result to the data array
    try {
      let parcels = (await axios.get(processingConfig.urlParcelData.href + '/lines', { params })).data
      commParcels.push(...parcels.results)
      while (parcels.results.length === 10000) {
        parcels = (await axios.get(parcels.next)).data
        commParcels.push(...parcels.results)
      }
    } catch (err) {
      console.log(err.error)
      console.log(err)
      await log.info('Paramètres de requête ' + JSON.stringify(params))
      throw err
    }
    // get the next string
    stringRequest = stringRequest.substring(firstIndex + 1 + lastValue, stringRequest.length)
  }
  if (stringRequest.length > 0) {
    // process the last group
    let firstIndex
    const indexslash = stringRequest.indexOf('/') !== -1 ? stringRequest.indexOf('/') : Infinity
    const indexpar = stringRequest.indexOf('(') !== -1 ? stringRequest.indexOf('(') : Infinity
    const indexbar = stringRequest.indexOf('|') !== -1 ? stringRequest.indexOf('|') : Infinity

    if (indexslash < indexpar && indexslash < indexbar) firstIndex = indexslash
    if (indexpar < indexslash && indexpar < indexbar) firstIndex = indexpar
    if (indexbar < indexslash && indexbar < indexpar) firstIndex = indexbar

    let decal = 1
    if (stringRequest[firstIndex + 1] === '/') decal = 2
    let tmpString = stringRequest.slice(firstIndex + decal, breakRequestAt)

    const lastValue = tmpString.lastIndexOf('|') > tmpString.lastIndexOf(')') ? tmpString.lastIndexOf('|') : tmpString.lastIndexOf(')')

    tmpString = tmpString.substring(0, lastValue)
    if (tmpString.startsWith(array[0].COMM)) tmpString = tmpString.substring(tmpString.indexOf('(') + 1, lastValue)
    params.qs = `code:(/${array[0].COMM}.{5}(${tmpString})/)`
    try {
      let parcels = (await axios.get(processingConfig.urlParcelData.href + '/lines', { params })).data
      commParcels.push(...parcels.results)
      while (parcels.results.length === 10000) {
        parcels = (await axios.get(parcels.next)).data
        commParcels.push(...parcels.results)
      }
    } catch (err) {
      console.log(err.error)
      console.log(err)
      await log.info('Paramètres de requête ' + JSON.stringify(params))
      throw err
    }
  }

  const duration = new Date().getTime() - start

  globalStats.moyReq += duration
  const stats = {
    sur: 0,
    premier: 0,
    geocode: 0,
    erreur: 0
  }
  const ret = []
  // const startTraitement = new Date().getTime()
  for (const input of array) {
    input.geocoding = {}
    input.geocoding.lat = input.latitude
    input.geocoding.lon = input.longitude
    input.geocoding.result_type = input.result_type

    delete input.latitude
    delete input.longitude
    delete input.result_type

    if (input.num_cadastre1.replace(/\D/g, '').length) {
      const codeParcelleTotal = commParcels.filter(elem => elem.code.match(new RegExp(`${array[0].COMM}.....${input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')}`)))
      let stoElem

      if (codeParcelleTotal !== undefined) {
        const matches = codeParcelleTotal.filter(s => s.code.substr(8, 2) === input.sec_cadastre1)
        if (matches.length > 0) {
          if (matches.length === 1) {
            stoElem = matches[0]
            stats.sur++
            input.latitude = parseFloat(stoElem.coord.split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem.coord.split(',')[1]).toFixed(6)
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem.code
          } else {
            let min, secondD
            for (const elem of matches) {
              const dist = measure(parseFloat(elem.coord.split(',')[0]), parseFloat(elem.coord.split(',')[1]), input.geocoding.lat, input.geocoding.lon)
              if (!min || dist < min) {
                min = dist
                stoElem = elem
                secondD = dist
              } else if (!secondD || dist < secondD) {
                secondD = dist
              }
            }
            const percent = Math.round(100 * secondD / (min + secondD))
            input.latitude = parseFloat(stoElem.coord.split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem.coord.split(',')[1]).toFixed(6)
            input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            input.parcelle = stoElem.code
            stats.geocode++
          }
        } else {
          if (codeParcelleTotal.length === 1) {
            stoElem = codeParcelleTotal[0]

            // On est sûr du résultat. Une seule parcelle disponible
            stats.sur++
            input.latitude = parseFloat(stoElem.coord.split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem.coord.split(',')[1]).toFixed(6)
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem.code
          } else if (codeParcelleTotal.length > 0 && (input.geocoding.result_type === 'housenumber' || input.geocoding.result_type === 'street')) {
            let min, secondD
            for (const elem of codeParcelleTotal) {
              const dist = measure(parseFloat(elem.coord.split(',')[0]), parseFloat(elem.coord.split(',')[1]), input.geocoding.lat, input.geocoding.lon)
              if (!min || dist < min) {
                min = dist
                stoElem = elem
                secondD = dist
              } else if (!secondD || dist < secondD) {
                secondD = dist
              }
            }
            const percent = Math.round(100 * secondD / (min + secondD))
            input.latitude = parseFloat(stoElem.coord.split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem.coord.split(',')[1]).toFixed(6)
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
                const dist = measure(parseFloat(elem.coord.split(',')[0]), parseFloat(elem.coord.split(',')[1]), input.geocoding.lat, input.geocoding.lon)
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
            input.latitude = parseFloat(stoElem.coord.split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem.coord.split(',')[1]).toFixed(6)
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
      input.latitude = undefined
      input.longitude = undefined
      input.parcel_confidence = undefined
      input.parcelle = undefined
      stats.erreur++
    }
    delete input.geocoding
    ret.push(input)
  }
  const sum = stats.sur + stats.geocode + stats.premier + stats.erreur
  await log.info(`Commune ${array[0].COMM}, ${array.length} parcelle(s), ${commParcels.length} récupérées en ${duration.toLocaleString('fr')} ms, Sûr : ${Math.round(stats.sur * 100 / sum)}%, Géocodé : ${Math.round(stats.geocode * 100 / sum)}%, Géododé peu précis : ${Math.round(stats.premier * 100 / sum)}%, Non défini : ${Math.round(stats.erreur * 100 / sum)}%`)
  globalStats.sur += stats.sur
  globalStats.geocode += stats.geocode
  globalStats.premier += stats.premier
  globalStats.erreur += stats.erreur

  if (!globalStats.header) {
    globalStats.header = true
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
  if (b === undefined) {
    await log.info(`Pas de fusion nécessaire, utilisation de ${a}. Option : ${option}`)
    let out = fs.createReadStream(a, { objectMode: true }).pipe(csv.parse({ columns: true, delimiter: ';' }))
    if (option.length > 0) {
      out = out.pipe(filter(function (data) {
        return option.includes(data.DEP)
      }, { objectMode: true }))
    }
    return out
  }

  await log.info(`Fusion de ${a} et ${b}. Option : ${option}`)

  const h1 = new Promise(function (resolve) {
    fs.createReadStream(a, { objectMode: true }).on('data', function (data) { resolve(data.toString().split('\n')[0]) })
  })

  const h2 = new Promise(function (resolve) {
    fs.createReadStream(b, { objectMode: true }).on('data', function (data) { resolve(data.toString().split('\n')[0]) })
  })

  if (await h1 !== await h2) {
    await log.error('Erreur : Les deux CSVs ne possèdent pas la même en-tête')
    throw new Error('Erreur : Les deux CSVs ne possèdent pas la même en-tête')
  } else {
    await log.info('Fichiers compatibles')
  }

  let f1 = fs.createReadStream(a, { objectMode: true }).pipe(csv.parse({ columns: true, delimiter: ';' }))

  let f2 = fs.createReadStream(b, { objectMode: true }).pipe(csv.parse({ columns: true, delimiter: ';' }))

  if (option.length > 0) {
    f1 = f1.pipe(filter(function (data) {
      return option.includes(data.DEP)
    }, { objectMode: true }))
    f2 = f2.pipe(filter(function (data) {
      return option.includes(data.DEP)
    }, { objectMode: true }))
  }

  return mergeSortStream(f1, f2, compare)
}

module.exports = async (pluginConfig, processingConfig, tmpDir, axios, log) => {
  const stats = {
    sur: 0,
    premier: 0,
    geocode: 0,
    erreur: 0,
    header: false,
    moyReq: 0
  }
  let currComm
  const batchComm = []

  const tab = []

  let files = await fs.readdir(tmpDir)
  await log.info(`Contenu répertoire de travail ${tmpDir} avant fusion : ${files}`)
  files = files.filter(file => file.endsWith('.csv') && file.includes(processingConfig.processFile) && !file.startsWith('sitadel'))
  await log.info(`Contenu filtré : ${files}`)
  const file1 = files[0] && path.join(tmpDir, files[0])
  const file2 = files[1] && path.join(tmpDir, files[1])
  await log.step('Traitement des fichiers')
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
          const result = await getParcel(batchComm, stats, pluginConfig, processingConfig, axios, log)
          currComm = obj.COMM
          batchComm.length = 0
          batchComm.push(obj)
          return next(null, result)
        }
        return next()
      },
      flush: async (callback) => {
        if (batchComm.length > 0) {
          const result = await getParcel(batchComm, stats, pluginConfig, processingConfig, axios, log)
          callback(null, result)
        }
      }
    }),
    csv.parse({ columns: true, delimiter: ',' }),
    csv.stringify({ header: true, quoted_string: true }),
    fs.createWriteStream(path.join(tmpDir, 'sitadel-' + processingConfig.processFile + '.csv'))
  )
  const sum = stats.sur + stats.geocode + stats.premier + stats.erreur
  await log.info(`Sûr : ${Math.round(stats.sur * 100 / sum)}%, Géocodé : ${Math.round(stats.geocode * 100 / sum)}%, Géododé peu précis : ${Math.round(stats.premier * 100 / sum)}%, Non défini : ${Math.round(stats.erreur * 100 / sum)}%, Total : ${sum}`)
  await log.info(`Moyenne requête parcelles : ${Math.round(stats.moyReq / sum)} ms`)
}
