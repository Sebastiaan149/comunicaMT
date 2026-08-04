import { Readable } from 'node:stream';
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { QuerySourceWiseKg } from '../../lib/QuerySourceWiseKg';

jest.mock<typeof import('hdt')>('hdt', () => ({ fromFile: jest.fn() }));

const DF = new DataFactory();
const BF = new BindingsFactory(<any> DF);
const AF = new AlgebraFactory(<any> DF);

const patterns = [
  AF.createPattern(
    DF.variable('item'),
    DF.namedNode('http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre'),
    DF.variable('genre'),
  ),
  AF.createPattern(
    DF.variable('item'),
    DF.namedNode('http://ogp.me/ns#title'),
    DF.variable('title'),
  ),
  AF.createPattern(
    DF.variable('item'),
    DF.namedNode('http://purl.org/stuff/rev#hasReview'),
    DF.variable('review'),
  ),
  AF.createPattern(
    DF.variable('item'),
    DF.namedNode('http://schema.org/description'),
    DF.variable('desc'),
  ),
  AF.createPattern(
    DF.variable('item'),
    DF.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
    DF.variable('type'),
  ),
  AF.createPattern(
    DF.variable('review'),
    DF.namedNode('http://purl.org/stuff/rev#rating'),
    DF.variable('rating'),
  ),
];

const expectedBgp = `?item <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?genre .
?item <http://ogp.me/ns#title> ?title .
?item <http://purl.org/stuff/rev#hasReview> ?review .
?item <http://schema.org/description> ?desc .
?item <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
?review <http://purl.org/stuff/rev#rating> ?rating .`;

const wisekgStep = {
  control: 'wisekg',
  star: {
    subject: '?review',
    triples: [
      { x: 'http://purl.org/stuff/rev#rating', y: '?rating' },
    ],
  },
};

describe('QuerySourceWiseKg', () => {
  let context: ActionContext;

  beforeEach(() => {
    context = new ActionContext({ [KeysInitQuery.dataFactory.name]: DF });
  });

  it('should serialize BGPs for /plan', () => {
    const source = createSource();
    expect((<any> source).serializeBgpForPlan(patterns)).toBe(expectedBgp);
  });

  it('should construct a /plan URL from the source origin', () => {
    const source = createSource('http://localhost:8080/smartkg');
    const url = (<any> source).buildWiseKgPlanUrl(patterns, context);

    expect(url).toBe(`http://localhost:8080/plan?bgp=${encodeURIComponent(expectedBgp)}&speed=1&latency=1000`);
  });

  it('should map partition controls to molecule HDT URLs', () => {
    const source = createSource('http://localhost:8080/smartkg');
    expect((<any> source).getPartitionHdtUrlForControl('partition/35'))
      .toBe('http://localhost:8080/molecule/smartkg/35.hdt');
  });

  it('should evaluate wisekg controls through the server source path', async() => {
    const source = createSource();
    const sourceAny = <any> source;
    const input = [ await sourceAny.emptyBinding() ];
    const sourceResult = BF.fromRecord({
      review: DF.namedNode('http://db.uwaterloo.ca/~galuc/wsdbm/Review1'),
      rating: DF.literal('5'),
    });

    jest.spyOn(sourceAny, 'queryStarViaControlledSource').mockImplementation(async() => [ sourceResult ]);
    jest.spyOn(sourceAny, 'fetchPartitionFileByControl').mockImplementation();

    const starPatterns = sourceAny.wiseKgStarToPatterns(wisekgStep.star);
    const results = await sourceAny.evaluateWiseKgStep(wisekgStep, starPatterns, input, context);

    expect(sourceAny.queryStarViaControlledSource).toHaveBeenCalledTimes(1);
    expect(sourceAny.fetchPartitionFileByControl).toHaveBeenCalledTimes(0);
    expect(results).toHaveLength(1);
    expect(results[0].get(DF.variable('rating'))?.equals(DF.literal('5'))).toBe(true);
  });

  it('should distinguish server-side partition controls from molecule shipping controls', () => {
    const sourceAny = <any> createSource();

    expect(sourceAny.isServerSourceControl('http://localhost:8080/partition/35')).toBe(true);
    expect(sourceAny.isPartitionShippingControl('http://localhost:8080/partition/35')).toBe(false);
    expect(sourceAny.isServerSourceControl('http://localhost:8080/molecule/wisekg/35.hdt')).toBe(false);
    expect(sourceAny.isPartitionShippingControl('http://localhost:8080/molecule/wisekg/35.hdt')).toBe(true);
  });

  it('should pass incoming bindings to server-side partition controls through SPF', async() => {
    const sourceAny = <any> createSource();
    const input = BF.fromRecord({ review: DF.namedNode('http://example.org/review') });
    const output = BF.fromRecord({
      item: DF.namedNode('http://example.org/item'),
      review: DF.namedNode('http://example.org/review'),
    });
    const queryForcedSourceBindings = jest.spyOn(sourceAny, 'queryForcedSourceBindings')
      .mockImplementation(async() => new ArrayIterator([ output ], { autoStart: false }));
    const step = {
      control: 'http://localhost:8080/partition/35',
      star: {
        subject: '?item',
        triples: [
          { x: 'http://purl.org/stuff/rev#hasReview', y: '?review' },
        ],
      },
    };

    const results = await sourceAny.queryStarViaControlledSource(
      sourceAny.wiseKgStarToPatterns(step.star),
      [ input ],
      step.control,
      context,
    );

    expect(queryForcedSourceBindings).toHaveBeenCalledTimes(1);
    expect(queryForcedSourceBindings.mock.calls[0][4]).toBe('spf');
    const queryOptions = <any> queryForcedSourceBindings.mock.calls[0][2];
    expect(queryOptions.joinBindings.metadata.cardinality.value).toBe(1);
    expect(results).toEqual([ output ]);
  });

  it('should fall back when /plan returns invalid JSON', async() => {
    const mediatorHttp = {
      mediate: jest.fn(async() => ({
        ok: true,
        status: 200,
        body: Readable.from([ 'not json' ]),
      })),
    };
    const source = createSource('http://localhost:8080/smartkg', mediatorHttp);
    const sourceAny = <any> source;
    const fallbackResult = BF.fromRecord({
      item: DF.namedNode('http://example.org/item'),
    });
    jest.spyOn(sourceAny, 'queryStarViaOriginalSource').mockImplementation(async() => [ fallbackResult ]);

    const results = await sourceAny.evaluateBgp(AF.createBgp([ patterns[0] ]), context);

    expect(mediatorHttp.mediate).toHaveBeenCalledTimes(3);
    expect(sourceAny.queryStarViaOriginalSource).toHaveBeenCalledTimes(1);
    expect(results).toHaveLength(1);
    expect(results[0].get(DF.variable('item'))?.equals(DF.namedNode('http://example.org/item'))).toBe(true);
  });
});

function createSource(url = 'http://localhost:8080/wisekg', mediatorHttp?: any): QuerySourceWiseKg {
  return new QuerySourceWiseKg(
    url,
    <any> DF,
    mediatorHttp ?? {
      mediate: jest.fn(async() => ({
        ok: true,
        status: 200,
        body: Readable.from([ '{}' ]),
      })),
    },
    new ActionContext({ [KeysInitQuery.dataFactory.name]: DF }),
    undefined,
  );
}
