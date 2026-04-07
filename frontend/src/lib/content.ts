export interface DatasetGuide {
  id: string;
  name: string;
  shortLabel: string;
  description: string;
  helper: string;
  starterQuestions: string[];
}

export const DATASET_GUIDES: DatasetGuide[] = [
  {
    id: 'government_finance',
    name: 'Government Finances',
    shortLabel: 'Gov Finance',
    description: 'State, county, and congressional-district fiscal position, liabilities, liquidity, revenue, and expenses.',
    helper: 'Best for liabilities, debt ratio, current ratio, revenue, expenses, and pension burdens.',
    starterQuestions: [
      'Which 15 states have the highest total liabilities per capita?',
      "Compare Maryland's total liabilities per capita to the national average and median.",
      "Where does Maryland rank nationally for debt ratio, and what are the top 10 and bottom 10?",
      "Compare Maryland's 3 highest-liability congressional districts to the state average (liabilities per capita).",
    ],
  },
  {
    id: 'acs',
    name: 'Census (ACS Demographics)',
    shortLabel: 'ACS',
    description: 'Demographic, income, education, poverty, and housing indicators across geographies.',
    helper: 'Best for median household income, poverty, education, and ownership patterns.',
    starterQuestions: [
      'Which 15 states have the highest poverty rates in 2023?',
      'Which 15 counties in California have the highest median household income in 2023?',
      "Compare Maryland's below poverty rate to the national average and median in 2023.",
      'Which 20 congressional districts have the highest median household income in 2023?',
    ],
  },
  {
    id: 'federal_spending',
    name: 'Federal Spending',
    shortLabel: 'Spending',
    description: 'Contracts, grants, direct payments, wages, residents, and employees across geographies.',
    helper: 'Best for spending totals, per-1000 measures, and national comparisons in 2024.',
    starterQuestions: [
      'Which 15 states received the most federal grants in 2024?',
      'Which 15 states received the most federal direct payments per 1000 in 2024?',
      'How does Maryland compare with the national average on grants per 1000 in 2024?',
      'Which 20 congressional districts received the most grants in 2024?',
    ],
  },
  {
    id: 'federal_spending_agency',
    name: 'Federal Spending by Agency',
    shortLabel: 'Agency',
    description: 'Agency-specific contract, grant, and direct-payment distributions for state views.',
    helper: 'Best for agency breakdowns like Department of Defense, HHS, Treasury, and Education.',
    starterQuestions: [
      'Which agencies provided the most grants in Maryland in 2024? Show top 10.',
      'Which agencies provided the most contracts in Virginia in 2024? Show top 10.',
      'How much did the Department of Defense spend in Maryland in 2024? Break out contracts, grants, and direct payments.',
      'Compare Department of Defense contracts across states in 2024. Show top 15 states.',
    ],
  },
  {
    id: 'finra',
    name: 'FINRA Financial Literacy',
    shortLabel: 'FINRA',
    description: 'Financial literacy, constraint, satisfaction, alternative financing, and risk aversion indicators.',
    helper: 'Best for household financial capability questions in 2021.',
    starterQuestions: [
      'Which 15 states rank highest on financial literacy in 2021?',
      'How does New York compare with the national average on financial constraint in 2021?',
      'Which 20 counties in Virginia have the highest financial literacy in 2021?',
      'Is financial literacy positively related to satisfaction across states in 2021?',
    ],
  },
  {
    id: 'fund_flow',
    name: 'Fund Flow',
    shortLabel: 'Flow',
    description: 'Subaward flows between recipient and subawardee geographies, with origin and destination detail.',
    helper: 'Best for flow destination rankings and outbound trend questions.',
    starterQuestions: [
      'Which states receive the most federal subaward funding from Maryland? Show top 15.',
      'Which states receive the smallest non-zero subaward amounts from California? Show bottom 10.',
      'How have Maryland outbound subaward totals changed by fiscal year?',
      'Which 20 counties receive the highest subaward amounts from Maryland recipient counties?',
    ],
  },
  {
    id: 'cross_dataset',
    name: 'Cross-Dataset Analysis',
    shortLabel: 'Cross',
    description: 'Questions that compare metrics across dashboards, such as poverty versus grants or literacy versus debt.',
    helper: 'Best for relationships, contrasts, and joined evidence across multiple datasets.',
    starterQuestions: [
      'Do states with higher financial literacy scores tend to have lower government debt ratios?',
      'Do states with higher poverty rates receive higher grants per 1000 in 2024?',
      'Compare financial literacy and poverty rates across states.',
      'Which congressional districts have high poverty rates but low contracts per 1000?',
    ],
  },
];

export const GENERAL_STARTERS = [
  'Which 15 states have the highest total liabilities per capita?',
  "Compare Maryland's total liabilities per capita to the national average and median.",
  'How much did the Department of Defense spend in Maryland in 2024? Break out contracts, grants, and direct payments.',
  'Which states receive the most federal subaward funding from Maryland? Show top 15.',
];
