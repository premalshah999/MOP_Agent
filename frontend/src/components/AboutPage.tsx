export function AboutPage() {
  return (
    <div className="flex-1 overflow-y-auto px-6 pb-16 pt-8">
      <article className="mx-auto w-full max-w-3xl space-y-8 text-[14px] leading-7 text-[var(--ink-soft)]">
        <header>
          <h1 className="font-display text-[28px] font-semibold text-[var(--ink)]">About MOP Agent</h1>
          <p className="mt-2 text-[13px] text-[var(--muted)]">
            A natural-language analyst over a fixed catalog of U.S. public-policy data.
          </p>
        </header>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">What it is</h2>
          <p className="mt-2">
            Ask plain-English questions about U.S. state, county, and congressional-district data —
            government finance, federal contracts and grants, demographics (ACS), FINRA financial-health
            indices, and federal subaward flows. The agent picks the right table, writes the SQL,
            executes it against a local DuckDB warehouse, and answers strictly from what the data returned.
          </p>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">Datasets covered</h2>
          <ul className="mt-2 space-y-1.5 list-disc pl-5">
            <li><strong>Government finance</strong> (FY2023): assets, liabilities, revenue, expenses, debt &amp; current ratios, pension burden — at state, county, and congressional-district level.</li>
            <li><strong>Federal contracts &amp; grants</strong> (2024 + 2020–2024 aggregate): contracts, grants, direct payments, resident wages, federal employees — at state, county, and CD level; also broken out by agency at the state level.</li>
            <li><strong>Demographics — Census ACS</strong> (annual, 2010–2023): population, race shares, education attainment, income brackets, poverty, housing.</li>
            <li><strong>FINRA financial health</strong> (survey waves: 2009, 2012, 2015, 2018, 2021 at the state level; 2021 only at county/CD): literacy, constraint, alternative financing, satisfaction, risk aversion (0–1 indices).</li>
            <li><strong>Federal subaward flows</strong>: directional dollar flows between geographies, by agency and industry.</li>
          </ul>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">How a question gets answered</h2>
          <ol className="mt-2 space-y-1.5 list-decimal pl-5">
            <li><strong>Intent</strong> — classify what's being asked (analytical question, clarification needed, off-catalog, meta).</li>
            <li><strong>Routing</strong> — pick the table(s) and key columns from the curated catalog.</li>
            <li><strong>Grounding</strong> — assemble the exact schema, critical warnings (state-name casing, year handling), and live resolved filter values for the chosen tables.</li>
            <li><strong>SQL + answer</strong> — write a DuckDB query, execute it through a read-only validator, then compose the answer strictly from the returned rows. A faithfulness judge checks the answer against the data.</li>
          </ol>
          <p className="mt-3">
            Toggle <em>Reasoning mode</em> (the lightbulb beside the send button) for harder questions —
            comparisons, distributional framing ("is X high?"), or chained drill-downs. In that mode the
            agent calls small tools (schema lookup, distinct values, peer statistics, run SQL) until it
            has enough evidence to answer.
          </p>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">Known limits</h2>
          <ul className="mt-2 space-y-1.5 list-disc pl-5">
            <li>Government-finance data is FY2023 only (no annual trend).</li>
            <li>FINRA county and congressional-district data exists for 2021 only.</li>
            <li>Contract / spending tables encode <code>year</code> as a string with two values: <code>'2024'</code> and <code>'2020-2024'</code> (5-year aggregate).</li>
            <li>State names are stored in different casings per table; comparisons need <code>LOWER()</code>.</li>
            <li>The agent answers only from the catalog above. Questions about metrics outside the catalog
              (crime, unemployment, GDP, forecasts) are explicitly declined.</li>
          </ul>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">Sources</h2>
          <p className="mt-2">
            USAspending.gov · U.S. Census Bureau (ACS 5-year estimates) ·
            FINRA Investor Education Foundation (National Financial Capability Study) ·
            curated by the University of Maryland Smith School (Maryland Opportunity Project).
          </p>
        </section>
      </article>
    </div>
  );
}
