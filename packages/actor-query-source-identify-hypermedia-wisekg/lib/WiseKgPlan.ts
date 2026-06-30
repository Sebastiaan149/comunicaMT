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

export function flattenWiseKgPlan(plan: IWiseKgPlanNode): IWiseKgExecutableStep[] {
  const steps: IWiseKgExecutableStep[] = [];
  const visit = (node: IWiseKgPlanNode | undefined): void => {
    if (!node || typeof node !== 'object') {
      return;
    }

    if (isWiseKgPlanOperator(node.operator)) {
      steps.push({
        control: node.operator.control,
        star: node.operator.star,
      });
    }

    visit(node.subplan);
  };

  visit(plan);
  return steps;
}

export function getWiseKgPlanExpiry(plan: IWiseKgPlanNode): number | undefined {
  return typeof plan.timestamp === 'number' && plan.timestamp > 0 ? plan.timestamp : undefined;
}

export function isWiseKgPlanNode(value: unknown): value is IWiseKgPlanNode {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const node = value as IWiseKgPlanNode;
  return node.operator === undefined || isWiseKgPlanOperator(node.operator);
}

function isWiseKgPlanOperator(value: unknown): value is IWiseKgPlanOperator {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const operator = value as IWiseKgPlanOperator;
  return typeof operator.control === 'string' && isWiseKgPlanStar(operator.star);
}

function isWiseKgPlanStar(value: unknown): value is IWiseKgPlanStar {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const star = value as IWiseKgPlanStar;
  return typeof star.subject === 'string' &&
    Array.isArray(star.triples) &&
    star.triples.every(triple => Boolean(triple) &&
      typeof triple === 'object' &&
      typeof (triple as IWiseKgPlanTriple).x === 'string' &&
      typeof (triple as IWiseKgPlanTriple).y === 'string');
}
