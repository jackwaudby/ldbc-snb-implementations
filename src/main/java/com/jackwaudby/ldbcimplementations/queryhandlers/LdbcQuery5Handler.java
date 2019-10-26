package com.jackwaudby.ldbcimplementations.queryhandlers;


/**
 * Title: New groups
 *
 * Description: Given a start Person, find the Forums which that Person’s friends and friends of friends
 * (excluding start Person) became Members of after a given date. For each forum find the number of Posts
 * that were created by any of these Persons. For each Forum and consider only those Persons which joined
 * that particular Forum after the given date.
 */
public class LdbcQuery5Handler {
}

// ["com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5",4398046514041,1292457600000,20]|
// [["Group for Queen_Victoria in Chengdu",1],
// ["Group for Augustine_of_Hippo in Fu'an",1],
// ["Group for Tony_Blair in Wuhan",1],
// ["Group for Michel_Foucault in Conghua",1],
// ["Group for Jackie_Chan in Quy_Nhơn",1],
// ["Group for Miguel_de_Cervantes in Oradea",1],
// ["Wall of Carmen Lepland",0],
// ["Wall of Hideki Yamada",0],
// ["Album 1 of Wuttichai Amornkiat",0],
// ["Wall of Ge Liu",0],
// ["Wall of Pol Dara",0],
// ["Wall of Macky Mendy",0],
// ["Wall of Aafia Khan",0],["Album 8 of Aafia Khan",0],["Wall of Bing Chen",0],["Wall of Jie Wang",0],["Wall of David Sanchez",0],["Wall of Chris Michie",0],["Wall of Jie Li",0],["Album 9 of Chen Yang",0]]

//g.V().has('Person','id',6597069768536).repeat(both('knows').simplePath()).emit().times(2).dedup().store('f').
//        inE('hasMember').has('joinDate',gt(new Date (1292457600000))).outV().as('forum').
//        group().by('title').by(out('containerOf').out('hasCreator').inE('hasMember').has('joinDate',gt(new Date (1292457600000))).count()).unfold()

// 330 friends
// became members of 6212 forums after given date
// contains 51341 posts
