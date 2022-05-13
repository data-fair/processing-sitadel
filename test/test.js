process.env.NODE_ENV = 'test'
const fs = require('fs-extra')
const config = require('config')
const axios = require('axios')
const chalk = require('chalk')
const moment = require('moment')
const assert = require('assert').strict
const processing = require('../')
const path = require('path')

describe('Station service processing', () => {
  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('should run a task', async function () {
    this.timeout(3600000)

    const headers = { 'x-apiKey': config.dataFairAPIKey }
    const axiosInstance = axios.create({
      baseURL: config.dataFairUrl,
      headers: headers,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    })

    // customize axios errors for shorter stack traces when a request fails
    axiosInstance.interceptors.response.use(response => response, error => {
      if (!error.response) return Promise.reject(error)
      delete error.response.request
      error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
      return Promise.reject(error.response)
    })

    const pluginConfig = { pluginMessage: 'Hello' }

    const processingConfig = {
      clearFiles: true,
      datasetMode: 'create',
      dataset: {
        title: 'Sitadel - test',
        id: 'sitadel-test-id'
      },
      departements: [56],
      processFile: 'demolir',
      urlParcelData: {
        href: config.parcelsUrl
      },
      tmpDir: 'data'
    }

    const log = {
      step: (msg) => console.log(chalk.blue.bold.underline(`[${moment().format('LTS')}] ${msg}`)),
      error: (msg, extra) => console.log(chalk.red.bold(`[${moment().format('LTS')}] ${msg}`), extra),
      warning: (msg, extra) => console.log(chalk.red(`[${moment().format('LTS')}] ${msg}`), extra),
      info: (msg, extra) => console.log(chalk.blue(`[${moment().format('LTS')}] ${msg}`), extra),
      debug: (msg, extra) => {
        console.log(`[${moment().format('LTS')}] ${msg}`, extra)
      }
    }
    const patchConfig = async (patch) => {
      console.log('received config patch', patch)
      Object.assign(processingConfig, patch)
    }

    const cwd = process.cwd()
    await fs.ensureDir(processingConfig.tmpDir)
    process.chdir(processingConfig.tmpDir)
    // console.log(process.cwd())
    await processing.run({ pluginConfig, processingConfig, tmpDir: path.resolve('./'), axios: axiosInstance, log, patchConfig })
    process.chdir(cwd)
  })
})
