const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const csv = require('csv')

function measure (lat1, lon1, lat2, lon2) { // generally used geo measurement function
  const R = 6378.137 // Radius of earth in KM
  const dLat = lat2 * Math.PI / 180 - lat1 * Math.PI / 180
  const dLon = lon2 * Math.PI / 180 - lon1 * Math.PI / 180
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  const d = R * c
  return d * 1000 // meters
}

const extend = async (processingConfig, axios, log) => {
  let currentDep = null
  let currentParcels = null
  let currentPrefixes = null
  let prevDep = null
  let prevParcels = null
  let prevPrefixes = null
  const counts = {
    empty: 0,
    noCanditates: 0,
    multipleCandidates: 0,
    noMatches: 0,
    multipleMatches: 0,
    oneMatch: 0
  }
  for (const filename of ['Permis_demolir.csv', 'Permis_amenager.csv', 'PC_DP_creant_locaux_2013_2016', 'PC_DP_creant_locaux_2017_2022.csv', 'PC_DP_creant_logements_2013_2016', 'PC_DP_creant_logements_2017_2022']) {
    // for (const filename of ['pa.csv', 'pd.csv', 'logements.csv', 'locaux.csv']) {
    console.log(filename)
    const total = {
      empty: 0,
      noCanditates: 0,
      multipleCandidates: 0,
      noMatches: 0,
      multipleMatches: 0,
      oneMatch: 0
    }
    await pump(
      fs.createReadStream(filename),
      csv.parse({ columns: true, delimiter: ';' }),
      csv.transform(async (input, done) => {
        const param = {
          params: {
            q: `${input.ADR_NUM_TER} ${input.ADR_LIBVOIE_TER} ${input.ADR_LIEUDIT_TER} ${input.ADR_LOCALITE_TER}`
          },
          headers: {
            'x-apiKey': processingConfig.geocoderApiKey
          }
        }
        try {
          const { lat, lon, matchLevel } = (await axios.get('https://koumoul.com/s/geocoder/api/v1/coord', param)).data
          input.geocoding = { lat, lon, matchLevel }
        } catch (err) {
          console.log('err geocoding', err)
          input.geocoding = { }
        }

        done(null, input)
      }),
      // sort(function (a, b) {
      //   console.log(a, b)
      //   return (a.DEP + '').localeCompare(b.DEP + '')
      // }),
      csv.transform(input => {
        if (parseInt(input.DEP) >= 2) return input
        if (input.num_cadastre1.trim().length) {
          if (input.DEP !== currentDep && input.DEP !== prevDep) {
            if (currentDep) console.log(currentDep, ...Object.entries(counts))
            console.log('Loading parcels for dep', input.DEP)
            Object.keys(counts).forEach(k => {
              total[k] += counts[k]
              counts[k] = 0
            })
            prevDep = currentDep
            prevParcels = currentParcels
            prevPrefixes = currentPrefixes
            currentDep = input.DEP
            currentParcels = JSON.parse(fs.readFileSync(path.join('parcelsCoords/' + currentDep + '.json'), 'utf-8'))
            currentPrefixes = JSON.parse(fs.readFileSync(path.join('prefixes/' + currentDep + '.json'), 'utf-8'))
          }
          const sections = input.DEP === currentDep ? currentPrefixes[input.COMM] : prevPrefixes[input.COMM]

          const depParcels = input.DEP === currentDep ? currentParcels : prevParcels
          if (!sections) console.log('no sections entry for', input.COMM)
          const canditates = Object.keys(sections || { '0000A': 0 }).filter(s => !input.sec_cadastre1 || input.sec_cadastre1.padStart(2, '0') === s.split('-').pop().padStart(2, '0')).map(s => {
            const [pre, sec] = s.split('-')
            return pre + sec.padStart(2, '0')
          })
          const matches = canditates.filter(s => depParcels[input.COMM + s + input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')])
          if (matches.length) {
            if (matches.length > 1 && (input.geocoding.matchLevel === 'housenumber' || input.geocoding.matchLevel === 'street')) {
              const dists = matches.map(s => {
                const coords = depParcels[input.COMM + s + input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')]
                return [s, measure(coords[1], coords[0], input.geocoding.lat, input.geocoding.lon)]
              })
              let firstD, secondD, firstS
              dists.forEach(([s, d]) => {
                if (!firstD || d < firstD) {
                  secondD = firstD
                  firstD = d
                  firstS = s
                } else if (!secondD || d < secondD) {
                  secondD = d
                }
              })
              input.parcelle = input.COMM + firstS + input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')
              const percent = Math.round(100 * secondD / (firstD + secondD))
              input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
              // console.log(input.parcel_confidence, firstS, dists)
            } else {
              input.parcelle = input.COMM + matches[0] + input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')
              const percent = Math.round(100 / matches.length)
              input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
            }
            const coords = depParcels[input.parcelle]
            input.latitude = coords[1]
            input.longitude = coords[0]
            if (matches.length === 1) counts.oneMatch++
            else counts.multipleMatches++
          } else {
            if (canditates.length >= 1) {
              input.parcelle = input.COMM + canditates[0] + input.num_cadastre1.replace(/\D/g, '').padStart(4, '0')
              const percent = Math.round(100 / canditates.length)
              input.parcel_confidence = (percent < 25 ? '<25' : (percent + '').padStart(3, '0')) + ' %'.padStart(2, '0')
              if (canditates.length === 1) counts.noMatches++
              else counts.multipleCandidates++
            } else {
              input.parcelle = undefined
              input.parcel_confidence = undefined
              counts.noCanditates++
            }
            if (input.geocoding.matchLevel === 'housenumber') {
              input.latitude = input.geocoding.lat
              input.longitude = input.geocoding.lon
            } else {
              input.latitude = undefined
              input.longitude = undefined
            }
          }
        } else {
          input.parcelle = undefined
          input.parcel_confidence = undefined
          if (input.geocoding.matchLevel === 'housenumber') {
            input.latitude = input.geocoding.lat
            input.longitude = input.geocoding.lon
          } else {
            input.latitude = undefined
            input.longitude = undefined
          }
          counts.empty++
        }
        delete input.geocoding
        return input
      }),
      csv.stringify({ header: true, quoted_string: true }),
      fs.createWriteStream(processingConfig.datasetIdPrefix + '-' + filename)
    )
    console.log('total', ...Object.entries(total))
  }
}

module.exports = async (processingConfig, axios, log) => {
  await extend(processingConfig, axios, log)
}
