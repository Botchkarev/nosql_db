
//creates 9 nodes, why?
//element id is the same, but id is different

CREATE (father1:Person {name: 'father1', age: 30});
CREATE (mother1:Person {name: 'mother1', age: 30});
CREATE (child1:Person {name: 'child1', age: 5});
//i think that (father1:Person {attributes}) creates a new dataset
//CREATE (father1:Person {name: 'father1'})-[:CHILD]->(child1:Person {name: 'child1'}) <-[:CHILD]-(mother1:Person {name: 'mother1'});
//CREATE (father1:Person {name: 'father1'})-[:FATHER_OF]->(child1:Person {name: 'child1'}) <-[:MOTHER_OF]-(mother1:Person {name: 'mother1'});



CREATE 
	(father1:Person {name: 'father1', age: 30}),
	(mother1:Person {name: 'mother1', age: 30}),
	(child:Person {name: 'child1', age: 5}),
	//does not work probably because father1 is declared in this context
	//(father1:Person {name: 'father1'})-[:CHILD]->(child1:Person {name: 'child1'}) <-[:CHILD]-(mother1:Person {name: 'mother1'}),
	//(father1:Person {name: 'father1'})-[:FATHER_OF]->(child1:Person {name: 'child1'}) <-[:MOTHER_OF]-(mother1:Person {name: 'mother1'});
	(father1)-[:FATHER_OF]->(child1) <-[:MOTHER_OF]-(mother1);



//actually works
CREATE
        (father1:Person {name: 'father1', age: 30}),
        (mother1:Person {name: 'mother1', age: 30}),
        (child1:Person {name: 'child1', age: 5}),
        (father1)-[:FATHER_OF]->(child1),
        (mother1)-[:MOTHER_OF]->(child1),
        (father1)-[:HUSBAND_OF]->(mother1),
        (mother1)-[:WIFE_OF]->(father1);
