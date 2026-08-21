import packageJson = require('../package.json');

describe('@comunica/actor-query-operation-bgp-wisekg', () => {
  it('should expose a valid Comunica module package', () => {
    expect(packageJson.name).toBe('@comunica/actor-query-operation-bgp-wisekg');
    expect(packageJson['lsd:module']).toBe(true);
    expect(packageJson.main).toBe('lib/index.js');
  });
});
