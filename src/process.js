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
const klaw = require('klaw-sync')

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

function sleep (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

async function retryGeocode (maxRetries, arr, axios, log) {
  return await geocode(arr, axios, log).catch(async function (err) {
    if (maxRetries <= 0) {
      await log.info(`Impossible de joindre api-adresse.data.gouv : ${err.status}, ${err.statusText}`)
      throw err
    }
    const ms = 60000 / maxRetries
    await log.info(`Erreur ${err.status} sur api-adresse.data.gouv : ${err.statusText}. Prochaine tentative dans ${ms.toFixed(0)} ms`)
    await sleep(ms)
    return await retryGeocode(maxRetries - 1, arr, axios, log)
  })
}

async function geocode (arr, axios, log) {
  await log.info(`Géocodage de ${arr.length} éléments`, '')
  const start = new Date().getTime()
  // add the header for the request
  const csvString = csvSync.stringify(arr, { header: true })

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

  let ret = response.data

  // in case of error during the geocoding, data.gouv seems to return the same file.
  // this part create a new response with a good format
  const headerRes = response.data.split('\n')[0]
  if (!(headerRes.includes('result_type') && headerRes.includes('latitude') && headerRes.includes('longitude'))) {
    await log.info('La réponse du géocodage ne possède pas les résultats')
    let newResponse
    for (const line of response.data.split('\n')) {
      if (line === headerRes) newResponse += line + ',result_type,latitude,longitude'
      else if (line !== '') newResponse += line + ',,,'
      if (line !== '') newResponse += '\n'
    }
    ret = newResponse
  }

  if (!header) {
    header = true
    return ret
  }
  // remove the header
  return ret.substring(ret.indexOf('\n') + 1)
}

async function getParcel (array, globalStats, keysParcelData, pluginConfig, processingConfig, axios, log) {
  const inseeCodeProp = {
    logements: 'Num_DAU',
    locaux: 'Num_DAU',
    amenager: 'Num_PA',
    demolir: 'Num_PD'
  }[processingConfig.processFile]

  const commCode = array[0][inseeCodeProp].charAt(0) === '0' ? array[0][inseeCodeProp].slice(1, 6) : array[0].COMM

  let stringRequest = ''
  let arrayUniNum = [...new Set(array.map(elem => elem.num_cadastre1.replace(/\D/g, '').padStart(4, '0').slice(-4)))]

  const ecart = 185
  do {
    stringRequest += `/${commCode}.{5}(${arrayUniNum.slice(0, ecart).join('|')})/`
    arrayUniNum = arrayUniNum.slice(ecart, arrayUniNum.length + 1)
  } while (arrayUniNum.length > ecart)
  if (arrayUniNum.length > 0) stringRequest += `/${commCode}.{5}(${arrayUniNum.slice(0, ecart).join('|')})/`

  const params = {
    size: 10000
  }

  // commParcels is the array containing all of the results of requests
  const commParcels = []

  const start = new Date().getTime()

  // To avoid URL overflow, break. Default at 2000
  const breakRequestAt = pluginConfig.urlLimit ? pluginConfig.urlLimit : 2000
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
    if (tmpString.startsWith(commCode)) tmpString = tmpString.substring(tmpString.indexOf('(') + 1, lastValue)
    params.qs = `code:(/${commCode}.{5}(${tmpString})/)`

    // process the requests and add the result to the data array
    try {
      let parcels = (await axios.get(processingConfig.urlParcelData.href + '/lines', { params })).data
      commParcels.push(...parcels.results)
      while (parcels.results.length === 10000) {
        parcels = (await axios.get(parcels.next)).data
        commParcels.push(...parcels.results)
      }
    } catch (err) {
      await log.info(`Une erreur est survenue sur la commune ${array[0].COMM} : ${err.status}, ${err.statusText}`)
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
    if (tmpString.startsWith(commCode)) tmpString = tmpString.substring(tmpString.indexOf('(') + 1, lastValue)
    params.qs = `code:(/${commCode}.{5}(${tmpString})/)`
    try {
      let parcels = (await axios.get(processingConfig.urlParcelData.href + '/lines', { params })).data
      commParcels.push(...parcels.results)
      while (parcels.results.length === 10000) {
        parcels = (await axios.get(parcels.next)).data
        commParcels.push(...parcels.results)
      }
    } catch (err) {
      await log.info(`Une erreur est survenue sur la commune ${array[0].COMM} : ${err.status}, ${err.statusText}`)
      await log.info('Paramètres de requête ' + JSON.stringify(params))
      throw err
    }
  }

  const duration = new Date().getTime() - start

  globalStats.moyReq += duration
  globalStats.sum += 1

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

    // unescape the '
    input.ADR_NUM_TER = input.ADR_NUM_TER.replaceAll('\\\'', '\'')
    input.ADR_LIBVOIE_TER = input.ADR_LIBVOIE_TER.replaceAll('\\\'', '\'')
    input.ADR_LIEUDIT_TER = input.ADR_LIEUDIT_TER.replaceAll('\\\'', '\'')
    input.ADR_LOCALITE_TER = input.ADR_LOCALITE_TER.replaceAll('\\\'', '\'')

    if (input.num_cadastre1.replace(/\D/g, '').length) {
      const codeParcelleTotal = commParcels.filter(elem => elem[keysParcelData.code].match(new RegExp(`${commCode}.....${input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')}`)))
      let stoElem

      if (codeParcelleTotal !== undefined) {
        const matches = codeParcelleTotal.filter(s => s[keysParcelData.code].substr(8, 2) === input.sec_cadastre1)
        if (matches.length > 0) {
          if (matches.length === 1) {
            stoElem = matches[0]
            stats.sur++
            input.latitude = parseFloat(stoElem[keysParcelData.coord].split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem[keysParcelData.coord].split(',')[1]).toFixed(6)
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem[keysParcelData.code]
          } else {
            let min, secondD
            for (const elem of matches) {
              const dist = measure(parseFloat(elem[keysParcelData.coord].split(',')[0]), parseFloat(elem[keysParcelData.coord].split(',')[1]), input.geocoding.lat, input.geocoding.lon)
              if (!min || dist < min) {
                min = dist
                stoElem = elem
                secondD = dist
              } else if (!secondD || dist < secondD) {
                secondD = dist
              }
            }
            const percent = Math.round(100 * secondD / (min + secondD))
            input.latitude = parseFloat(stoElem[keysParcelData.coord].split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem[keysParcelData.coord].split(',')[1]).toFixed(6)
            input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            input.parcelle = stoElem[keysParcelData.code]
            stats.geocode++
          }
        } else {
          if (codeParcelleTotal.length === 1) {
            stoElem = codeParcelleTotal[0]

            // On est sûr du résultat. Une seule parcelle disponible
            stats.sur++
            input.latitude = parseFloat(stoElem[keysParcelData.coord].split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem[keysParcelData.coord].split(',')[1]).toFixed(6)
            input.parcel_confidence = 100 + ' %'
            input.parcelle = stoElem[keysParcelData.code]
          } else if (codeParcelleTotal.length > 0 && (input.geocoding.result_type === 'housenumber' || input.geocoding.result_type === 'street')) {
            let min, secondD
            for (const elem of codeParcelleTotal) {
              const dist = measure(parseFloat(elem[keysParcelData.coord].split(',')[0]), parseFloat(elem[keysParcelData.coord].split(',')[1]), input.geocoding.lat, input.geocoding.lon)
              if (!min || dist < min) {
                min = dist
                stoElem = elem
                secondD = dist
              } else if (!secondD || dist < secondD) {
                secondD = dist
              }
            }
            const percent = Math.round(100 * secondD / (min + secondD))
            input.latitude = parseFloat(stoElem[keysParcelData.coord].split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem[keysParcelData.coord].split(',')[1]).toFixed(6)
            input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            input.parcelle = stoElem[keysParcelData.code]
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
                const dist = measure(parseFloat(elem[keysParcelData.coord].split(',')[0]), parseFloat(elem[keysParcelData.coord].split(',')[1]), input.geocoding.lat, input.geocoding.lon)
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
            input.latitude = parseFloat(stoElem[keysParcelData.coord].split(',')[0]).toFixed(6)
            input.longitude = parseFloat(stoElem[keysParcelData.coord].split(',')[1]).toFixed(6)
            input.parcel_confidence = confidence
            input.parcelle = stoElem[keysParcelData.code]
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

    if (globalStats.stoHeader) {
      const diffHeader = globalStats.stoHeader.filter((elem) => !Object.keys(input).some((elem2) => elem === elem2))
      for (const col of diffHeader) {
        await log.info(`Il manque la colonne ${col}.`)
        input[col] = undefined
      }
    }
    ret.push(input)
  }
  const sum = stats.sur + stats.geocode + stats.premier + stats.erreur
  await log.info(`Commune ${array[0].COMM}, ${array.length} parcelle(s), ${commParcels.length} récupérées en ${duration.toLocaleString('fr')} ms, Sûr : ${Math.round(stats.sur * 100 / sum)}%, Géocodé : ${Math.round(stats.geocode * 100 / sum)}%, Géododé peu précis : ${Math.round(stats.premier * 100 / sum)}%, Non défini : ${Math.round(stats.erreur * 100 / sum)}%`)
  globalStats.sur += stats.sur
  globalStats.geocode += stats.geocode
  globalStats.premier += stats.premier
  globalStats.erreur += stats.erreur

  // console.log(ret)
  if (!globalStats.header) {
    globalStats.header = true
    globalStats.stoHeader = Object.keys(ret[0])
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

let fusionDone = false

async function fusion (a, b, option, log) {
  if (!fusionDone) {
    fusionDone = true
    let cpt = 0
    if (option.length > 0) await log.info(`Option(s) de fusion : ${option}`)
    if (b === undefined) {
      await log.info(`Pas de fusion nécessaire, utilisation de ${a}.`)
      let out = fs.createReadStream(a, { objectMode: true }).pipe(csv.parse({ columns: true, delimiter: ';' }))
      out = out.pipe(filter(function (data) {
        if (option.includes(data.DEP)) cpt++
        return option.length ? option.includes(data.DEP) : true
      }, { objectMode: true }))
      out.on('finish', async () => {
        if (cpt === 0 && option.length > 0) {
          await log.info(`Aucun résultat avec le filtre ${option}`)
          throw new Error('Rien à traiter')
        }
      })
      return out
    }

    await log.info(`Fusion de ${a} et ${b}.`)

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
        if (option.includes(data.DEP)) cpt++
        return option.length ? option.includes(data.DEP) : true
      }, { objectMode: true }))
      f2 = f2.pipe(filter(function (data) {
        if (option.includes(data.DEP)) cpt++
        return option.length ? option.includes(data.DEP) : true
      }, { objectMode: true }))
    }

    f1.on('finish', () => {
      f2.on('finish', async () => {
        if (cpt === 0 && option.length > 0) {
          await log.info(`Aucun résultat avec le filtre ${option}`)
          throw new Error('Rien à traiter')
        }
      })
    })
    return mergeSortStream(f1, f2, compare)
  }
}

module.exports = async (pluginConfig, processingConfig, tmpDir, axios, log) => {
  const stats = {
    sur: 0,
    premier: 0,
    geocode: 0,
    erreur: 0,
    moyReq: 0,
    sum: 0,
    header: false,
    stoHeader: undefined
  }
  let currComm
  let batchComm = []
  let tab = []
  const keysParcelData = {}

  try {
    const schemaUrlParcelData = (await axios.get(processingConfig.urlParcelData.href + '/schema')).data
    for (const i of schemaUrlParcelData) {
      if (i['x-refersTo'] === 'http://www.w3.org/2003/01/geo/wgs84_pos#lat_long') keysParcelData.coord = i.key
      if (i['x-refersTo'] === 'http://dbpedia.org/ontology/codeLandRegistry') keysParcelData.code = i.key
    }

    // let files = await fs.readdir(tmpDir)
    let files = klaw(tmpDir).sort((f1, f2) => f2.stats.size - f1.stats.size)
    files = files.map(f => f.path)

    // await log.info(`Contenu répertoire de travail ${tmpDir} avant fusion : ${files}`)
    files = files.filter(file => file.endsWith('.csv') && file.includes(processingConfig.processFile))
    // await log.info(`Contenu filtré : ${files}`)
    await log.step('Traitement des fichiers')
    await pump(
      await fusion(files[0], files[1], processingConfig.departements, log),
      new stream.Transform({
        objectMode: true,
        transform: async (obj, _, next) => {
          // escape the ' with a \ to avoid issue during the geocoding
          obj.ADR_NUM_TER = obj.ADR_NUM_TER.replaceAll('\'', '\\\'')
          obj.ADR_LIBVOIE_TER = obj.ADR_LIBVOIE_TER.replaceAll('\'', '\\\'')
          obj.ADR_LIEUDIT_TER = obj.ADR_LIEUDIT_TER.replaceAll('\'', '\\\'')
          obj.ADR_LOCALITE_TER = obj.ADR_LOCALITE_TER.replaceAll('\'', '\\\'')
          tab.push(obj)
          if (tab.length >= 10000) {
            const result = await retryGeocode(5, tab, axios, log)
            tab = []
            return next(null, result)
          }
          return next()
        },
        flush: async (callback) => {
          if (tab.length > 0) {
            const result = await retryGeocode(5, tab, axios, log)
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
            const result = await getParcel(batchComm, stats, keysParcelData, pluginConfig, processingConfig, axios, log)
            currComm = obj.COMM
            batchComm = []
            batchComm.push(obj)
            return next(null, result)
          }
          return next()
        },
        flush: async (callback) => {
          if (batchComm.length > 0) {
            const result = await getParcel(batchComm, stats, keysParcelData, pluginConfig, processingConfig, axios, log)
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
    await log.info(`Moyenne requête parcelles : ${Math.round(stats.moyReq / stats.sum)} ms`)
  } catch (err) {
    await log.info(`Erreur : ${err.statusText}`)
    throw err
  }
}
