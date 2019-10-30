package com.jackwaudby.ldbcimplementations.queryhandlers;

public class LdbcQuery10Handler {
}

// TODO: add city name to query
// TODO: test in query test bed
// TODO: write handler
// TODO: set_01
// TODO: set_02

//                g.V().has('Person','id',2199023256020).sideEffect(out('hasInterest').store('tags')).
//                        repeat(both('knows').simplePath()).times(2).dedup().
//                        where(values('birthday').map{it.get().getMonth()}.is(eq(1-1)).
//                        and().values('birthday').map{it.get().getDate()}.is(gte(21)).
//                        or().values('birthday').map{it.get().getMonth()}.is(eq(2-1)).
//                        and().values('birthday').map{it.get().getDate()}.is(lt(22))).
//                        local(union(
//                        __.in('hasCreator').hasLabel('Post').where(out('hasTag').dedup().where(within('tags')).count().is(gt(0))).count(),
//                        __.in('hasCreator').hasLabel('Post').count(),
//                        values('id','firstName','lastName')
//                        ).fold()).
//                        map{it -> [commonInterestScore:it.get().getAt(0) - (it.get().getAt(1)-it.get().getAt(0)) ,id:it.get().getAt(2),firstName:it.get().getAt(3),lastName:it.get().getAt(4)]}.
//                        order().by(select('commonInterestScore'),desc).by(select('id'),asc).limit(10)
//
//



