package com.jackwaudby.ldbcimplementations.queryhandlers;


/**
 * Given a start Person, find the Comments that this Person’s friends made in reply to Posts.
 * Considering only those Comments that are immediate (1-hop) replies to Posts, not the transitive (multi-hop) case.
 * Only consider Posts with a Tag in a given TagClass or in a descendent of that TagClass.
 * Count the number of these reply Comments.
 * Collect the Tags that were attached to the Posts they replied to,
 *  but only collect Tags with the given TagClass or with a descendant of that TagClass
 * Return Persons with at least one reply, the reply count, and the collection of Tags.
 */

public class LdbcQuery12Handler {



}
