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
// g.V().sideEffect(hasLabel('Tag').where(out('hasType').has('TagClass','name','GolfPlayer')).aggregate('cond')).has('Person','id',3298534885557).as('person').both('knows').as('friend').where(__.in('hasCreator').hasLabel('Comment').out('replyOf').hasLabel('Post').out('hasTag').where(within('cond'))).as('person').select('person').by(__.in('hasCreator').hasLabel('Comment').out('replyOf').hasLabel('Post').out('hasTag').where(within('cond')).dedup().values('name').fold()).as('tagNames').select('person').by(__.in('hasCreator').hasLabel('Comment').out('replyOf').hasLabel('Post').out('hasTag').where(within('cond')).values('name').count().fold()).as('replyCount').select('person','tagNames','replyCount').by(valueMap('id','firstName','lastName')).by().by()

public class LdbcQuery12Handler {



}
