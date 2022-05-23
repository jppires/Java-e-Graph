package br.edu.univas.tcc;

import java.util.Date;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;


public class GraphDB {
	private static enum RelTypes implements RelationshipType
	{
	    CONHECE,RESIDE,PARTICIPA,TRABALHA,ESTUDA,GOSTA
	}

	private static int pessoas = 1000;
	private static int cidades = 4;
	private static int grupos = 5;
	private static int escolas = 4;
	private static int interesses = 10;
	private static int empresas = 4;
	
	private static int limitePessoas = 10;
	private static int limiteGrupos = 3;
	private static int limiteInteresses = 5;

	static Transaction tx;
	
	public static void main(String[] args) {

		GraphDatabaseService graphDb;
		
		Node listPessoas[] = new Node[pessoas];
		Node listCidades[] = new Node[cidades];
		Node listInteresses[] = new Node[interesses];
		Node listGrupos[] = new Node[grupos];
		Node listEscolas[] = new Node[escolas];
		Node listEmpresas[] = new Node[empresas];
		Random generator = new Random();
		
      // graphDb = new GraphDatabaseFactory().newEmbeddedDatabase("C:/neo4j-community-1.9.2/data/graph.db");
		
		graphDb = new GraphDatabaseFactory().
			    newEmbeddedDatabaseBuilder( "C:/neo4j-community-1.9.2-windows_temp/neo4j-community-1.9.2/data/graph.db").
			    setConfig( GraphDatabaseSettings.node_keys_indexable, "name,vertice" ).
			    setConfig( GraphDatabaseSettings.node_auto_indexing, "true" ).
			    newGraphDatabase();
   
		tx = graphDb.beginTx();
		try
		{
				
			//pessoas
			System.out.println("Criando pessoas...");
			for(int i=0;i<pessoas;i++){
				int sexo = (i+1);
				listPessoas[i] = graphDb.createNode();
				listPessoas[i].setProperty( "name", "nome"+i);
				listPessoas[i].setProperty( "idade", generator.nextInt(55));
				listPessoas[i].setProperty( "vertice", "pessoas");
				listPessoas[i].setProperty( "email", "email"+i);
				listPessoas[i].setProperty( "sexo", (sexo%2==0?"M":"F"));
				intermediateCommit(graphDb);
			}


			//cidades
			System.out.println("Criando cidades...");
			for(int i=0;i<cidades;i++){
				listCidades[i] = graphDb.createNode();
				listCidades[i].setProperty( "name", "cidade"+i);
				listCidades[i].setProperty( "vertice", "cidades");
				intermediateCommit(graphDb);
			}
			
			
			//interesses
			System.out.println("Criando interesses...");
			for(int i=0;i<interesses;i++){
				listInteresses[i] = graphDb.createNode();
				listInteresses[i].setProperty( "name", "interesse"+i);
				listInteresses[i].setProperty( "vertice", "interesse");
				intermediateCommit(graphDb);
			}
			
			//grupos
			System.out.println("Criando grupos...");
			for(int i=0;i<grupos;i++){
				listGrupos[i] = graphDb.createNode();
				listGrupos[i].setProperty( "name", "grupo"+i);
				listGrupos[i].setProperty( "vertice", "grupo");
				intermediateCommit(graphDb);
			}

			//escolas
			System.out.println("Criando escolas...");
			for(int i=0;i<escolas;i++){
				listEscolas[i] = graphDb.createNode();
				listEscolas[i].setProperty( "name", "escola"+i);
				listEscolas[i].setProperty( "vertice", "escola");
				intermediateCommit(graphDb);
			}

			//empresas
			System.out.println("Criando empresas...");
			for(int i=0;i<empresas;i++){
				listEmpresas[i] = graphDb.createNode();
				listEmpresas[i].setProperty( "name", "empresa"+i);
				listEmpresas[i].setProperty( "vertice", "empresa");
				intermediateCommit(graphDb);
			}
			
			//relacionamentos
			System.out.println("Relacionando pessoas...");
			for(int i=0;i<pessoas;i++){
				Integer list[] = getNodes(pessoas,limitePessoas,i);
			   	for(int k=0;k<6;k++){
					listPessoas[i].createRelationshipTo(listPessoas[list[k]],RelTypes.CONHECE).setProperty("tipo", "familia");
				}
				for(int k=6;k<limitePessoas;k++){
					listPessoas[i].createRelationshipTo(listPessoas[list[k]],RelTypes.CONHECE).setProperty("tipo", "amigo");
				}
				intermediateCommit(graphDb);
				System.out.println(i);
			}

			//relacionando interesses
			System.out.println("Relacionando interesses...");
			for(int i=0;i<pessoas;i++){
				Integer list[] = getNodes(interesses,limiteInteresses,interesses+1);
			   	for(int k=0;k<limiteInteresses;k++){
					listPessoas[i].createRelationshipTo(listInteresses[list[k]],RelTypes.GOSTA);
				}
			   	System.out.println("interesse"+i);
			   	intermediateCommit(graphDb);
			}


			//relacionando grupo
			System.out.println("Relacionando grupos...");
			for(int i=0;i<pessoas;i++){
				Integer list[] = getNodes(grupos,limiteGrupos,grupos+1);
			   	for(int k=0;k<limiteGrupos;k++){
					listPessoas[i].createRelationshipTo(listGrupos[list[k]],RelTypes.PARTICIPA);
				}
				System.out.println("grupo"+i);
				intermediateCommit(graphDb);
			}
			

			//relacionando cidades
			System.out.println("Relacionando cidades...");
			for(int i=0;i<pessoas;i++){
				int nodeCidade = generator.nextInt(cidades);
				listPessoas[i].createRelationshipTo(listCidades[nodeCidade],RelTypes.RESIDE);
				System.out.println("cidades"+i);
				intermediateCommit(graphDb);
			}
			
			//relacionando pessoa-escola
			System.out.println("Relacionando escolas...");
			for(int i=0;i<pessoas;i++){
				int nodeEscola = generator.nextInt(escolas);
				listPessoas[i].createRelationshipTo(listEscolas[nodeEscola],RelTypes.ESTUDA);
				System.out.println("escolas"+i);
				intermediateCommit(graphDb);
			}

			//relacionando escola-cidade
			for(int i=0;i<escolas;i++){
				int nodeCidade = generator.nextInt(cidades);
				listEscolas[i].createRelationshipTo(listCidades[nodeCidade],RelTypes.RESIDE);
				intermediateCommit(graphDb);
			}
			
			//relacionando empresa-cidade
			for(int i=0;i<empresas;i++){
				int nodeCidade = generator.nextInt(cidades);
				listEmpresas[i].createRelationshipTo(listCidades[nodeCidade],RelTypes.RESIDE);
				intermediateCommit(graphDb);
			}
		
			//relacionando empresa
			System.out.println("Relacionando empresas...");
			for(int i=0;i<pessoas;i++){
				int nodeEmpresa = generator.nextInt(empresas);
				listPessoas[i].createRelationshipTo(listEmpresas[nodeEmpresa],RelTypes.TRABALHA);
				intermediateCommit(graphDb);
				System.out.println("empresa"+i);
			}

			
			
			System.out.print( "CONCLUÍDO em: "+new Date());
			
			
		    tx.success();
		}
		finally
		{
		    tx.finish();
		}
		graphDb.shutdown();
		

	}
	
	static int count = 0;

	private static void intermediateCommit(GraphDatabaseService graph){
		if(count % 1000 == 0) {
		    tx.success();
		    tx.finish();
			tx = graph.beginTx();
		}
		count++;	
	}
	public static Integer[] getNodes(int populacao, int limite, int individuo){
		Integer[] nodes = new Integer[limite];
		Random generator = new Random();
		
		for(int i=0;i<limite;i++){
			nodes[i] = 0;
		}
		
		for(int i=0; i<limite; i++){
			int existNode = 0;
			int selectedNode = generator.nextInt(populacao);
				for(int k=0;k<nodes.length;k++){
					if(nodes[k] == selectedNode || selectedNode == individuo){
						existNode = 1;
						break;
					}
				}
			if(existNode == 0){
				nodes[i] = selectedNode;
			}else{
				i--;
			}
		}
	    return nodes;
	}
}
