import packageJson = require('../package.json');

describe('@comunica/actor-query-source-identify-hypermedia-spf', () => {
  it('should expose a valid Comunica module package', () => {
    expect(packageJson.name).toBe('@comunica/actor-query-source-identify-hypermedia-spf');
    expect(packageJson['lsd:module']).toBe(true);
    expect(packageJson.main).toBe('lib/index.js');
  });
});
