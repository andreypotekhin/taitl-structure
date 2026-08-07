"""Resolve caller-owned bands into reusable user-band contexts."""

from typing import Final

from examples.search.schemas.cohorts.resolve import BandAncestor, BandMatch, SingletonUserBand, UserBandPath
from examples.search.schemas.user import Band, BandFallback, BandMembership, User, UserBand, UserBandMembership
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    array,
    array_contains,
    collect_list,
    concat_ws,
    cross_join,
    drop_duplicates,
    group_by,
    hierarchy_closure,
    hierarchy_fallbacks,
    inner_join,
    left_join,
    not_exists,
    relation_alias,
    require_all,
    require_parent_hierarchy,
    require_unique,
    sha2,
    size,
    union_all,
    when,
    where,
)


class ResolveCohortBands(Transform):
    """Map profiles to band memberships and ordered user-band fallbacks."""

    maximum_band_depth: Final = 8

    users = input(User)
    bands = input(Band)
    valid_bands = lane(Band)
    matches = lane(BandMatch)
    leaf_matches = lane(BandMatch)
    band_ancestors = lane(BandAncestor)
    user_band_paths = lane(UserBandPath)
    resolved_user_bands = lane(UserBand)
    singleton_user_bands = lane(SingletonUserBand)
    singleton_catalog = lane(UserBand)
    direct_band_memberships = lane(BandMembership)
    resolved_band_memberships = lane(BandMembership)
    band_memberships = output(BandMembership)
    user_bands = output(UserBand)
    user_band_memberships = output(UserBandMembership)
    band_fallbacks = output(BandFallback)

    @step(input=bands, output=valid_bands)
    def validate_bands(self, band: Band) -> Band:
        require_unique(band.id)
        require_all(band.age_start.is_null() | band.age_end.is_null() | (band.age_start < band.age_end))
        require_parent_hierarchy(
            band.id,
            parent=band.parent_band_id,
            order_by=band.priority,
            max_depth=self.maximum_band_depth,
        )
        return Band.project(band)

    @step(input=[users, valid_bands], output=matches)
    def match_bands(self, user: User, band: Band) -> BandMatch:
        cross_join(band, allow_cartesian=True)
        where(
            ((size(band.genders) == 0) | array_contains(band.genders, user.gender))
            & ((size(band.locales) == 0) | array_contains(band.locales, user.locale))
            & ((size(band.countries) == 0) | array_contains(band.countries, user.country))
            & ((size(band.geo_tags) == 0) | array_contains(band.geo_tags, user.geo_tag))
            & ((size(band.device_types) == 0) | array_contains(band.device_types, user.device_type))
            & ((size(band.time_zones) == 0) | array_contains(band.time_zones, user.time_zone))
            & (band.age_start.is_null() | (user.age >= band.age_start))
            & (band.age_end.is_null() | (user.age < band.age_end))
        )
        return BandMatch(
            user_id=user.id,
            band_id=band.id,
            priority=band.priority,
            parent_band_id=band.parent_band_id,
        )

    @step(input=matches, output=leaf_matches)
    def select_leaf_matches(self, match: BandMatch) -> BandMatch:
        child = relation_alias(match, name="child_match")
        where(
            not_exists(
                child,
                on=(child.user_id == match.user_id) & (child.parent_band_id == match.band_id),
            )
        )
        return BandMatch.project(match)

    @step(input=valid_bands, output=band_ancestors)
    def expand_band_ancestors(self, band: Band) -> BandAncestor:
        ancestors = hierarchy_closure(
            band.id,
            parent=band.parent_band_id,
            as_=BandAncestor,
            node="band_id",
            ancestor="ancestor_band_id",
            max_depth=self.maximum_band_depth,
            scope="band_ancestors",
        )
        return BandAncestor.project(ancestors)

    @step(input=leaf_matches, output=user_band_paths)
    def build_user_band_paths(self, match: BandMatch) -> UserBandPath:
        group_by(user_id=match.user_id)
        return UserBandPath(
            user_id=match.user_id,
            band_ids=collect_list(match.band_id, order_by=match.priority.desc()),
        )

    @step(input=user_band_paths, output=resolved_user_bands)
    def build_resolved_user_bands(self, path: UserBandPath) -> UserBand:
        drop_duplicates(path.band_ids)
        return UserBand(user_band_id=sha2(concat_ws("\u001f", path.band_ids)), band_ids=path.band_ids)

    @step(input=valid_bands, output=singleton_user_bands)
    def build_singleton_user_bands(self, band: Band) -> SingletonUserBand:
        return SingletonUserBand(
            band_id=band.id,
            user_band_id=sha2(band.id),
            band_ids=array(band.id),
        )

    @step(input=singleton_user_bands, output=singleton_catalog)
    def publish_singleton_user_bands(self, singleton: SingletonUserBand) -> UserBand:
        return UserBand(user_band_id=singleton.user_band_id, band_ids=singleton.band_ids)

    @step(input=[resolved_user_bands, singleton_catalog], output=user_bands)
    def merge_user_band_catalog(self, resolved: UserBand, singleton: UserBand) -> UserBand:
        catalog = union_all(singleton)
        drop_duplicates(catalog.user_band_id, catalog.band_ids)
        return UserBand.project(catalog)

    @step(input=[users, user_band_paths], output=user_band_memberships)
    def build_user_band_memberships(self, user: User, path: UserBandPath) -> UserBandMembership:
        left_join(path, on=path.user_id == user.id)
        return UserBandMembership(
            user_id=user.id,
            user_band_id=when(path.user_id.is_not_null(), sha2(concat_ws("\u001f", path.band_ids))).otherwise(None),
        )

    @step(input=[leaf_matches, band_ancestors, singleton_user_bands], output=direct_band_memberships)
    def build_direct_band_memberships(
        self, match: BandMatch, ancestor: BandAncestor, singleton: SingletonUserBand
    ) -> BandMembership:
        inner_join(ancestor, on=ancestor.band_id == match.band_id)
        inner_join(singleton, on=singleton.band_id == match.band_id)
        return BandMembership(
            user_id=match.user_id,
            band_id=ancestor.ancestor_band_id,
            user_band_id=singleton.user_band_id,
        )

    @step(input=user_band_memberships, output=resolved_band_memberships)
    def build_resolved_band_memberships(self, membership: UserBandMembership) -> BandMembership:
        where(membership.user_band_id.is_not_null())
        return BandMembership(
            user_id=membership.user_id,
            band_id=None,
            user_band_id=membership.user_band_id,
        )

    @step(input=[direct_band_memberships, resolved_band_memberships], output=band_memberships)
    def merge_band_memberships(self, direct: BandMembership, resolved: BandMembership) -> BandMembership:
        return BandMembership.project(union_all(resolved))

    @step(input=[user_bands, valid_bands], output=band_fallbacks)
    def build_band_fallbacks(self, user_band: UserBand, band: Band) -> BandFallback:
        fallbacks = hierarchy_fallbacks(
            user_band.user_band_id,
            user_band.band_ids,
            band,
            parent_id=band.id,
            parent=band.parent_band_id,
            as_=BandFallback,
            max_depth=self.maximum_band_depth,
            scope="band_fallbacks",
        )
        return BandFallback.project(fallbacks)
