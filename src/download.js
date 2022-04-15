const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const exec = util.promisify(require('child_process').exec)
const config = require('config')

module.exports = async (pluginConfig, dir = 'data', axios, log) => {

  await log.step('Téléchargement du fichier instantané')
  
  const fileName = path.parse(new URL(pluginConfig.url).pathname).name + '.zip'
  const file = `${dir}/${fileName}`

  console.log(`download ${url} -> ${file}`)
  const cmd = `wget -N -nv --no-check-certificate --retry-connrefused --waitretry=60 --timeout=120 -O ${fileName} "${url}"`

  await exec(cmd, `${process.cwd()}/${dir}/`)
  console.log(`unzip ${file}`)
  await exec(`unzip -o ${file}`)
  // remove the zip file
  await fs.remove(fileName)
}