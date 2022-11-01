package com.floragunn.searchguard.enterprise.dlsfls.legacy;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.junit.Assert;

public class DlsTermsLookupAsserts {

    /**
     * Asserts that the source map of a search hit contains an field that contains access codes
     * and asserts those access codes contain at least on of the access codes for the user
     *
     * @param sourceMap
     * @param accessCodesKey
     * @param userCodes
     */
    public static void assertAccessCodesMatch(Map<String, Object> sourceMap, String accessCodesKey, Integer[] userCodes) {
        Object field = sourceMap.get(accessCodesKey);
        Assert.assertTrue(sourceMap.toString(), field instanceof Collection<?>);
        Collection<?> documentAccessCodes = (Collection<?>) field;
        // make sure the access codes in the document contain at least one code of the user access codes
        Assert.assertTrue(sourceMap.toString(), documentAccessCodes.removeAll(Arrays.asList(userCodes)));

    }

    /**
     * Extracts the access codes from search hits and compares them with a given collection of
     * access codes. The documents access codes are retrieved for each document from the 'access_codes' field.
     * This method asserts that those access codes contain at least on of the access codes for the user
     *
     * @param searchHits the search hits from the tlqdocuments index
     * @param userCodes  the access coded of the user
     */
    public static void assertAccessCodesMatch(Collection<Hit<Map>> searchHits, Integer[] userCodes) {
        for (Hit<Map> hit : searchHits) {
            assertAccessCodesMatch(hit.source(), "access_codes", userCodes);
        }
    }

    /**
     * See above
     *
     * @param searchHits
     * @param userCodes
     */
    public static void assertAccessCodesMatch(Hit<Map>[] searchHits, Integer[] userCodes) {
        assertAccessCodesMatch(Arrays.asList(searchHits), userCodes);
    }

    /**
     * Checks whether a document from the tlqdocuments index contains a certain value in the bu field
     *
     * @param searchHit
     * @param buCode
     */
    public static void assertBuMatches(Hit<Map> searchHit, String buCode) {
        Object field = searchHit.source().get("bu");
        Assert.assertTrue(searchHit.toString(), field instanceof String);
        Assert.assertTrue(searchHit.toString(), ((String) field).equals(buCode));
    }

    /**
     * Checks whether all document from the tlqdocuments index contains a certain value in the bu field
     *
     * @param searchHits
     * @param buCode
     */
    public static void assertBuMatches(Collection<Hit<Map>> searchHits, String buCode) {
        for (Hit<Map> searchHit : searchHits) {
            assertBuMatches(searchHit, buCode);
        }
    }

    /**
     * Compares the cluster alias field in search hits with a given alias and fails
     * if the alias name is different
     *
     * @param searchHits
     * @param clusterAlias
     */
    public static void assertAllHitsComeFromCluster(Collection<Hit<Map>> searchHits, String clusterAlias) {
        assertTrue("Expected cluster alias name to not be null", clusterAlias != null);
        for (Hit<Map> hit : searchHits) {
            final String cluster = hit.index().split(":")[0];
            assertTrue("Expected cluster alias in search hit to not be null\n" + hit, cluster != null);
            assertTrue(hit.toString(), cluster.equals(clusterAlias));
        }
    }

    public static void assertAllHitsComeFromCluster(Hit<Map>[] searchHits, String clusterAlias) {
        assertAllHitsComeFromCluster(Arrays.asList(searchHits), clusterAlias);
    }

    public static void assertAllHitsComeFromLocalCluster(Hit<Map>[] searchHits) {
        assertAllHitsComeFromLocalCluster(Arrays.asList(searchHits));
    }

    public static void assertAllHitsComeFromLocalCluster(Collection<Hit<Map>> searchHits) {
        for (Hit<Map> hit : searchHits) {
            final String cluster = hit.index().split(":")[0];
            assertTrue(hit.toString(), cluster.equals(hit.index()));
        }
    }

    public static Optional<StringTermsBucket> getBucket(StringTermsAggregate agg, String bucketName) {
        if(agg.buckets().isKeyed()) {
            return Optional.ofNullable(agg.buckets().keyed().get(bucketName));
        }

        return agg.buckets().array().stream().filter(p->p.key().stringValue().equals(bucketName)).findFirst();
    }
}
