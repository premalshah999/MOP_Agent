export function AboutPage() {
  return (
    <div className="flex-1 overflow-y-auto px-6 pb-16 pt-8">
      <article className="mx-auto w-full max-w-3xl space-y-8 text-[14px] leading-7 text-[var(--ink-soft)]">
        <header>
          <h1 className="font-display text-[28px] font-semibold text-[var(--ink)]">
            About Maryland Opportunity
          </h1>
          <p className="mt-2 text-[13px] text-[var(--muted)]">
            A conversational research assistant for a curated catalog of U.S. public-policy data.
          </p>
        </header>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
            What you can explore
          </h2>
          <p className="mt-2">
            Ask questions about states, counties, and congressional districts in plain language.
            The catalog covers government finances, federal spending and agencies, Census
            demographics, household financial capability, and federal subaward flows. You can
            request totals, rankings, comparisons, trends where annual data is available, and
            cross-dataset analysis.
          </p>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
            Data coverage
          </h2>
          <ul className="mt-2 list-disc space-y-1.5 pl-5">
            <li><strong>Government finances:</strong> FY2023 fiscal position and burden measures at state, county, and congressional-district levels.</li>
            <li><strong>Federal spending:</strong> 2024 and a 2020–2024 aggregate for contracts, grants, direct payments, wages, residents, and employees; state-level agency detail is also available.</li>
            <li><strong>Census ACS:</strong> annual demographic, income, education, poverty, and housing indicators from 2010 through 2023.</li>
            <li><strong>FINRA financial capability:</strong> state survey waves from 2009 through 2021; county and congressional-district views for 2021.</li>
            <li><strong>Federal subaward flows:</strong> origin and destination totals across states, counties, and congressional districts; annual coverage varies by geography.</li>
          </ul>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
            Evidence and interpretation
          </h2>
          <p className="mt-2">
            Answers are generated from read-only queries over the catalog. When available, each
            response includes the underlying query, returned rows, time scope, assumptions, and
            methodology notes. Extended analysis adds comparative or multi-step evidence checks
            for harder questions. If a requested measure is not in the catalog, the assistant
            identifies the gap instead of substituting a different measure.
          </p>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
            Important coverage notes
          </h2>
          <ul className="mt-2 list-disc space-y-1.5 pl-5">
            <li>Government-finance data is a FY2023 snapshot, so annual trends are not available.</li>
            <li>FINRA county and congressional-district views are available for 2021 only.</li>
            <li>State-level subaward flows are a catalog snapshot without an annual field; county and congressional flow tables support annual analysis.</li>
            <li>Results reflect the definitions and coverage of the source datasets and may include revisions or negative award adjustments.</li>
          </ul>
        </section>

        <section>
          <h2 className="text-[14px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
            Sources
          </h2>
          <p className="mt-2">
            USAspending.gov · U.S. Census Bureau American Community Survey · FINRA Investor
            Education Foundation National Financial Capability Study · University of Maryland
            Smith School, Maryland Opportunity Project.
          </p>
        </section>
      </article>
    </div>
  );
}
