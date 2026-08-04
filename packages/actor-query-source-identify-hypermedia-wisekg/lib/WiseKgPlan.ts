export interface IWiseKgPlanTriple {
  x: string;
  y: string;
}

export interface IWiseKgPlanStar {
  subject: string;
  triples: IWiseKgPlanTriple[];
}

export interface IWiseKgPlanOperator {
  control: string;
  star: IWiseKgPlanStar;
}

export interface IWiseKgPlanNode {
  operator?: IWiseKgPlanOperator;
  subplan?: IWiseKgPlanNode;
  timestamp?: number;
}

export interface IWiseKgExecutableStep {
  control: string;
  star: IWiseKgPlanStar;
}

export interface IWiseKgFetchedPlan {
  plan: IWiseKgPlanNode;
  steps: IWiseKgExecutableStep[];
  expiresAt?: number;
}

// Flatten a nested WiseKG plan into the ordered executable steps.
export function flattenWiseKgPlan(plan: IWiseKgPlanNode): IWiseKgExecutableStep[] {
  const steps: IWiseKgExecutableStep[] = [];
  const visit = (node: IWiseKgPlanNode | undefined): void => {
    if (!node || typeof node !== 'object') {
      return;
    }

    visit(node.subplan);

    if (isWiseKgPlanOperator(node.operator)) {
      steps.push({
        control: node.operator.control,
        star: node.operator.star,
      });
    }
  };

  visit(plan);
  return steps;
}

// Read the server-provided plan expiry timestamp when present.
export function getWiseKgPlanExpiry(plan: IWiseKgPlanNode): number | undefined {
  return typeof plan.timestamp === 'number' && plan.timestamp > 0 ? plan.timestamp : undefined;
}

// Validate that a parsed JSON value has the expected WiseKG plan shape.
export function isWiseKgPlanNode(value: unknown): value is IWiseKgPlanNode {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const node = <IWiseKgPlanNode> value;
  return node.operator === undefined || isWiseKgPlanOperator(node.operator);
}

// Validate a single plan operator before executing it.
function isWiseKgPlanOperator(value: unknown): value is IWiseKgPlanOperator {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const operator = <IWiseKgPlanOperator> value;
  return typeof operator.control === 'string' && isWiseKgPlanStar(operator.star);
}

// Validate the star pattern payload inside a WiseKG plan operator.
function isWiseKgPlanStar(value: unknown): value is IWiseKgPlanStar {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const star = <IWiseKgPlanStar> value;
  return typeof star.subject === 'string' &&
    Array.isArray(star.triples) &&
    star.triples.every(triple => Boolean(triple) &&
      typeof triple === 'object' &&
      typeof (triple).x === 'string' &&
      typeof (triple).y === 'string');
}
