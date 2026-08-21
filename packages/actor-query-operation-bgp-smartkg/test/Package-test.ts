import packageJson = require('../package.json');

describe('@comunica/actor-query-operation-bgp-smartkg', () => {
  it('should expose a valid Comunica module package', () => {
    expect(packageJson.name).toBe('@comunica/actor-query-operation-bgp-smartkg');
    expect(packageJson['lsd:module']).toBe(true);
    expect(packageJson.main).toBe('lib/index.js');
  });
});
