#!/usr/bin/env node
/* eslint-disable import/no-nodejs-modules */

import * as path from 'node:path';
import { HttpServiceSparqlEndpoint } from '@comunica/actor-init-query';

const process: NodeJS.Process = require('process/');

const moduleRootPath = path.join(__dirname, '..');
const defaultConfigPath = path.join(moduleRootPath, 'config', 'config-default.json');

HttpServiceSparqlEndpoint.runArgsInProcess(
  process.argv.slice(2),
  process.stdout,
  process.stderr,
  moduleRootPath,
  process.env,
  defaultConfigPath,
  code => process.exit(code),
).catch(error => process.stderr.write(`${error.message}\n`));
