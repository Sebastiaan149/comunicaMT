import {
  flattenWiseKgPlan,
  getWiseKgPlanExpiry,
  type IWiseKgPlanNode,
} from '../../lib/WiseKgPlan';

const examplePlan: IWiseKgPlanNode = {
  operator: {
    control: 'partition/35',
    star: {
      subject: '?item',
      triples: [
        { x: 'http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre', y: '?genre' },
        { x: 'http://ogp.me/ns#title', y: '?title' },
        { x: 'http://purl.org/stuff/rev#hasReview', y: '?review' },
        { x: 'http://schema.org/description', y: '?desc' },
        { x: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', y: '?type' },
      ],
    },
  },
  subplan: {
    operator: {
      control: 'wisekg',
      star: {
        subject: '?review',
        triples: [
          { x: 'http://purl.org/stuff/rev#rating', y: '?rating' },
        ],
      },
    },
    subplan: { timestamp: 0 },
    timestamp: 1_782_842_923_149,
  },
  timestamp: 1_782_842_923_153,
};

describe('WiseKgPlan', () => {
  describe('#flattenWiseKgPlan', () => {
    it('should flatten root-first annotated plan steps', () => {
      const steps = flattenWiseKgPlan(examplePlan);

      expect(steps).toHaveLength(2);
      expect(steps[0].control).toBe('partition/35');
      expect(steps[0].star.subject).toBe('?item');
      expect(steps[0].star.triples).toHaveLength(5);
      expect(steps[1].control).toBe('wisekg');
      expect(steps[1].star.subject).toBe('?review');
      expect(steps[1].star.triples).toHaveLength(1);
    });
  });

  describe('#getWiseKgPlanExpiry', () => {
    it('should return the positive root timestamp', () => {
      expect(getWiseKgPlanExpiry(examplePlan)).toBe(1_782_842_923_153);
    });

    it('should ignore terminal timestamp zero', () => {
      expect(getWiseKgPlanExpiry({ timestamp: 0 })).toBeUndefined();
    });
  });
});
