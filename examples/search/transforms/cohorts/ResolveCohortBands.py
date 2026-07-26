"""Resolve caller-owned bands into reusable user-band contexts."""

from examples.search.schemas.user import Band, BandFallback, BandMembership, User, UserBand, UserBandMembership
from structure import Transform, input, output, raw, step
from structure.plugin.pyspark import array


class ResolveCohortBands(Transform):
    """Map profiles to band memberships and ordered user-band fallbacks.

    Recursive parent traversal is intentionally isolated in the raw hook because the
    current Structure DSL has no recursive relation operation. All ordinary Search
    transforms consume its typed outputs rather than repeating hierarchy logic.
    """

    users = input(User)
    bands = input(Band)
    band_memberships = output(BandMembership)
    user_bands = output(UserBand)
    user_band_memberships = output(UserBandMembership)
    band_fallbacks = output(BandFallback)

    @step(input=users, output=[band_memberships, user_bands, user_band_memberships, band_fallbacks])
    def declare_outputs(self, user: User) -> tuple[BandMembership, UserBand, UserBandMembership, BandFallback]:
        """Declare resolver output contracts before the raw recursive hook replaces them."""

        return (
            BandMembership(user_id=user.id, band_id=None, user_band_id=""),
            UserBand(user_band_id="", band_ids=array(user.id)),
            UserBandMembership(user_id=user.id, user_band_id=None),
            BandFallback(user_band_id="", user_band_fallback_id=None, ordinal=0),
        )

    @raw(
        input=[input(users), input(bands)],
        output=[output(band_memberships), output(user_bands), output(user_band_memberships), output(band_fallbacks)],
    )
    def resolve_bands(self, *, users, bands, band_memberships, user_bands, user_band_memberships, band_fallbacks, spark, ctx):
        """Materialize deterministic leaf memberships and priority-tail fallback chains."""

        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        # Bands are deliberately a bounded caller-owned configuration catalog. The
        # catalog is collected so hierarchy validation and traversal stay explicit.
        catalog = bands.select("id", "parent_band_id", "priority", "age_start", "age_end").collect()
        ids = [row.id for row in catalog]
        duplicates = sorted({cohort_id for cohort_id in ids if ids.count(cohort_id) > 1})
        if duplicates:
            raise ValueError(f"Band IDs must be unique: {', '.join(duplicates)}")
        invalid_ages = sorted(
            row.id for row in catalog if row.age_start is not None and row.age_end is not None and row.age_start >= row.age_end
        )
        if invalid_ages:
            raise ValueError(f"Band age_start must be less than age_end: {', '.join(invalid_ages)}")
        parents = {row.id: row.parent_band_id for row in catalog}
        priorities = {row.id: row.priority for row in catalog}
        missing = sorted(parent for parent in parents.values() if parent is not None and parent not in parents)
        if missing:
            raise ValueError(f"Band parent_band_id refers to missing band IDs: {', '.join(missing)}")
        insufficient = sorted(
            cohort_id
            for cohort_id, parent_id in parents.items()
            if parent_id is not None and priorities[cohort_id] <= priorities[parent_id]
        )
        if insufficient:
            raise ValueError(f"Band child priority must exceed its parent priority: {', '.join(insufficient)}")
        for cohort_id in parents:
            seen = set()
            current = cohort_id
            while current is not None:
                if current in seen:
                    raise ValueError(f"Band parent_band_id contains a cycle at band ID: {current}")
                seen.add(current)
                current = parents[current]

        user = users.alias("user")
        band = bands.alias("band")
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
            F.col("band.id").alias("band_id"),
            F.col("band.priority"),
            F.col("band.parent_band_id"),
        )
        child = matches.alias("child")
        parent = matches.alias("parent")
        leaf = child.join(
            parent,
            (F.col("child.user_id") == F.col("parent.user_id"))
            & (F.col("parent.parent_band_id") == F.col("child.band_id")),
            "left_anti",
        ).select("child.*")
        membership = leaf.select("user_id", "band_id", "priority", "parent_band_id")

        def ancestors(band_id):
            result = []
            while band_id is not None:
                result.append(band_id)
                band_id = parents[band_id]
            return result

        lineage_type = T.ArrayType(T.StringType(), containsNull=False)
        lineage = membership.select(
            "band_id", F.explode(F.udf(ancestors, lineage_type)("band_id")).alias("ancestor_band_id")
        ).dropDuplicates()
        ordered = membership.groupBy("user_id").agg(
            F.sort_array(F.collect_list(F.struct((-F.col("priority")).alias("order"), "band_id"))).alias("ordered")
        ).select("user_id", F.transform("ordered", lambda item: item.band_id).alias("band_ids"))
        resolved_users = users.select(F.col("id").alias("user_id")).join(ordered, "user_id", "left")
        resolved_users = resolved_users.withColumn(
            "user_band_id",
            F.when(F.col("band_ids").isNotNull(), F.sha2(F.concat_ws("\u001f", "band_ids"), 256)),
        )
        resolved_bands = resolved_users.where(F.col("user_band_id").isNotNull()).select(
            "user_band_id", "band_ids"
        ).dropDuplicates()
        singleton_bands = bands.select(
            F.sha2(F.concat_ws("\u001f", F.array("id")), 256).alias("user_band_id"), F.array("id").alias("band_ids")
        )
        resolved_membership = membership.join(lineage, "band_id").join(
            singleton_bands.select(F.col("user_band_id").alias("singleton_user_band_id"), F.element_at("band_ids", 1).alias("band_id")),
            "band_id",
        ).select("user_id", F.col("ancestor_band_id").alias("band_id"), F.col("singleton_user_band_id").alias("user_band_id")).dropDuplicates()
        resolved_membership = resolved_membership.unionByName(
            resolved_users.where(F.col("user_band_id").isNotNull()).select(
                "user_id", F.lit(None).cast("string").alias("band_id"), "user_band_id"
            )
        )

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
        all_bands = resolved_bands.unionByName(singleton_bands).dropDuplicates()
        chains = all_bands.withColumn("chain", F.udf(chain, chain_type)("band_ids"))
        fallback = chains.select("user_band_id", F.posexplode("chain").alias("ordinal", "fallback_band_ids")).select(
            "user_band_id",
            F.when(F.size("fallback_band_ids") > 0, F.sha2(F.concat_ws("\u001f", "fallback_band_ids"), 256)).alias(
                "user_band_fallback_id"
            ),
            F.col("ordinal").cast("long").alias("ordinal"),
        )
        user_band_catalog = all_bands.select("user_band_id", "band_ids")
        user_membership = resolved_users.select("user_id", "user_band_id")
        return resolved_membership, user_band_catalog, user_membership, fallback
