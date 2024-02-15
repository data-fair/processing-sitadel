process.env.NODE_ENV = 'test'
const config = require('config')
const assert = require('assert').strict
const processing = require('../')

describe('Sitadel processing', () => {
  it('should expose a plugin config schema for super admins', async () => {
    const schema = require('../plugin-config-schema.json')
    assert.ok(schema)
  })
  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('should run a task', async function () {
    this.timeout(3600000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: { urlLimit: 2000 },
      processingConfig: {
        datasetMode: 'update',
        forceUpdate: true,
        dataset: {
          title: 'Sitadel - test',
          id: 'sitadel-test-id'
        },
        departements: ['56'],
        processFile: 'demolir',
        urlParcelData: {
          href: config.parcelsUrl
        },
        clearFiles: false
      },
      tmpDir: 'data'
    }, config, false)
    await processing.run(context)
  })
})
