import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { translate } from 'sparqlalgebrajs';
import { Shapes } from '../lib/Shapes';

describe('Shapes', () => {
  describe('PASSAGE', () => {
    it('should accept spo operations', () => {
      expect(doesShapeAcceptOperation(
        Shapes.PASSAGE,
        translate('SELECT * WHERE { ?s ?p ?o }'),
      )).toBeTruthy();
    });
    it('should accept bgp operations', () => {
      expect(doesShapeAcceptOperation(
        Shapes.PASSAGE,
        translate('SELECT * WHERE { ?person <http://own> ?animal . ?animal <http://species> ?species }'),
      )).toBeTruthy();
    });

    it('should transform the sequence path so it gets accepted', () => {
      expect(doesShapeAcceptOperation(
        Shapes.PASSAGE,
        translate('SELECT * WHERE { ?person <http://own>/<http://species> ?species }'),
      )).toBeTruthy();
    });

    it('does not handle group by queries yet', () => {
      expect(doesShapeAcceptOperation(
        Shapes.PASSAGE,
        translate('SELECT ?p WHERE { ?p <http://own> ?a } GROUP BY ?p'),
      )).toBeFalsy();
    });
    it('does not handle property path yet', () => {
      expect(doesShapeAcceptOperation(
        Shapes.PASSAGE,
        translate('SELECT * WHERE { ?person <http://own>+ ?species }'),
      )).toBeFalsy();
    });

    // TODO: Eventually, we would like to identify too large operations
    //       to keep them on the smart-client. The trade-off being that
    //       the Passage server indeed does additional computation, but
    //       at least, the FILTERs comprising thousands of checks are not
    //       doing round-trips in the network.
    //       However, not sure it's possible with the Shapes as for now.
    //       Possibly possible with the parser that should arrive in
    //       Comunica 5.
  });
});
