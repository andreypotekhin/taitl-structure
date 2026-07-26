"""Resolve caller-owned cohorts into reusable Search bands."""

from examples.search.schemas.user import Band, BandFallback, Cohort, CohortLineage, CohortMembership, User, UserBand
from structure import Transform, input, output, raw, step
from structure.plugin.pyspark import array


class ResolveCohortBands(Transform):
    """Map profiles to cohort memberships, reusable bands, and ordered fallbacks.

    Recursive parent traversal is intentionally isolated in the raw hook because the
    current Structure DSL has no recursive relation operation. All ordinary Search
    transforms consume its typed outputs rather than repeating hierarchy logic.
    """

    users = input(User)
    cohorts = input(Cohort)
    cohort_memberships = output(CohortMembership)
    cohort_lineage = output(CohortLineage)
    bands = output(Band)
    user_bands = output(UserBand)
    band_fallbacks = output(BandFallback)

    @step(input=users, output=[cohort_memberships, cohort_lineage, bands, user_bands, band_fallbacks])
    def declare_outputs(
        self, user: User
    ) -> tuple[CohortMembership, CohortLineage, Band, UserBand, BandFallback]:
        """Declare resolver output contracts before the raw recursive hook replaces them."""

        return (
            CohortMembership(user_id=user.id, cohort_id="", priority=0, parent_cohort_id=None),
            CohortLineage(cohort_id="", ancestor_cohort_id=""),
            Band(band_id="", cohort_ids=array(user.id)),
            UserBand(user_id=user.id, band_id=None),
            BandFallback(band_id="", fallback_band_id=None, ordinal=0),
        )

    @raw(
        input=[input(users), input(cohorts)],
        output=[output(cohort_memberships), output(cohort_lineage), output(bands), output(user_bands), output(band_fallbacks)],
    )
    def resolve_bands(self, *, users, cohorts, cohort_memberships, cohort_lineage, bands, user_bands, band_fallbacks, spark, ctx):
        """Materialize deterministic leaf memberships and priority-tail fallback chains."""

        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        # Cohorts are deliberately a bounded caller-owned configuration catalog. The
        # catalog is collected so hierarchy validation and traversal stay explicit.
        catalog = cohorts.select("id", "parent_cohort_id", "priority", "age_start", "age_end").collect()
        ids = [row.id for row in catalog]
        duplicates = sorted({cohort_id for cohort_id in ids if ids.count(cohort_id) > 1})
        if duplicates:
            raise ValueError(f"Cohort IDs must be unique: {', '.join(duplicates)}")
        invalid_ages = sorted(
            row.id for row in catalog if row.age_start is not None and row.age_end is not None and row.age_start >= row.age_end
        )
        if invalid_ages:
            raise ValueError(f"Cohort age_start must be less than age_end: {', '.join(invalid_ages)}")
        parents = {row.id: row.parent_cohort_id for row in catalog}
        priorities = {row.id: row.priority for row in catalog}
        missing = sorted(parent for parent in parents.values() if parent is not None and parent not in parents)
        if missing:
            raise ValueError(f"Cohort parent_cohort_id refers to missing cohort IDs: {', '.join(missing)}")
        insufficient = sorted(
            cohort_id
            for cohort_id, parent_id in parents.items()
            if parent_id is not None and priorities[cohort_id] <= priorities[parent_id]
        )
        if insufficient:
            raise ValueError(f"Cohort child priority must exceed its parent priority: {', '.join(insufficient)}")
        for cohort_id in parents:
            seen = set()
            current = cohort_id
            while current is not None:
                if current in seen:
                    raise ValueError(f"Cohort parent_cohort_id contains a cycle at cohort ID: {current}")
                seen.add(current)
                current = parents[current]

        user = users.alias("user")
        band = cohorts.alias("band")
        matches = user.crossJoin(band).where(
            ((F.size(F.col("band.genders")) == 0) | F.array_contains(F.col("band.genders"), F.col("user.gender")))
            & ((F.size(F.col("band.locales")) == 0) | F.array_contains(F.col("band.locales"), F.col("user.locale")))
            & ((F.size(F.col("band.countries")) == 0) | F.array_contains(F.col("band.countries"), F.col("user.country")))
            & ((F.size(F.col("band.geo_tags")) == 0) | F.array_contains(F.col("band.geo_tags"), F.col("user.geo_tag")))
            & ((F.size(F.col("band.device_types")) == 0) | F.array_contains(F.col("band.device_types"), F.col("user.device_type")))
            & ((F.size(F.col("band.time_zones")) == 0) | F.array_contains(F.col("band.time_zones"), F.col("user.time_zone")))
            & (F.col("band.age_start").isNull() | (F.col("user.age") >= F.col("band.age_start")))
            & (F.col("band.age_end").isNull() | (F.col("user.age") < F.col("band.age_end")))
        ).select(
            F.col("user.id").alias("user_id"),
            F.col("band.id").alias("cohort_id"),
            F.col("band.priority"),
            F.col("band.parent_cohort_id"),
        )
        child = matches.alias("child")
        parent = matches.alias("parent")
        leaf = child.join(
            parent,
            (F.col("child.user_id") == F.col("parent.user_id"))
            & (F.col("parent.parent_cohort_id") == F.col("child.cohort_id")),
            "left_anti",
        ).select("child.*")
        membership = leaf.select("user_id", "cohort_id", "priority", "parent_cohort_id")

        def ancestors(cohort_id):
            result = []
            while cohort_id is not None:
                result.append(cohort_id)
                cohort_id = parents[cohort_id]
            return result

        lineage_type = T.ArrayType(T.StringType(), containsNull=False)
        lineage = membership.select(
            "cohort_id", F.explode(F.udf(ancestors, lineage_type)("cohort_id")).alias("ancestor_cohort_id")
        ).dropDuplicates()
        ordered = membership.groupBy("user_id").agg(
            F.sort_array(F.collect_list(F.struct((-F.col("priority")).alias("order"), "cohort_id"))).alias("ordered")
        ).select("user_id", F.transform("ordered", lambda item: item.cohort_id).alias("cohort_ids"))
        resolved_users = users.select(F.col("id").alias("user_id")).join(ordered, "user_id", "left")
        resolved_users = resolved_users.withColumn(
            "band_id",
            F.when(F.col("cohort_ids").isNotNull(), F.sha2(F.concat_ws("\u001f", "cohort_ids"), 256)),
        )
        resolved_bands = resolved_users.where(F.col("band_id").isNotNull()).select(
            "band_id", "cohort_ids"
        ).dropDuplicates()

        def chain(ids):
            current = list(ids)
            result = []
            while current:
                result.append(current)
                parent_id = parents[current[-1]]
                if parent_id is None:
                    current = current[:-1]
                elif parent_id in current[:-1]:
                    current = current[:-1]
                else:
                    current = [*current[:-1], parent_id]
            result.append([])
            return result

        chain_type = T.ArrayType(T.ArrayType(T.StringType(), containsNull=False), containsNull=False)
        chains = resolved_bands.withColumn("chain", F.udf(chain, chain_type)("cohort_ids"))
        fallback = chains.select("band_id", F.posexplode("chain").alias("ordinal", "fallback_cohort_ids")).select(
            "band_id",
            F.when(F.size("fallback_cohort_ids") > 0, F.sha2(F.concat_ws("\u001f", "fallback_cohort_ids"), 256)).alias(
                "fallback_band_id"
            ),
            F.col("ordinal").cast("long").alias("ordinal"),
        )
        return membership, lineage, resolved_bands, resolved_users.select("user_id", "band_id"), fallback
