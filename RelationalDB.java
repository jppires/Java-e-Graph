package br.edu.univas.tcc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

public class RelationalDB {
	
	static Connection conn = null;
	
	private static int usuarios = 1000000;
	private static int grupos = 30;
	
	//ATENÇÃO QUANTO AO NUMERO, USAR MULTIPLO DE 4 + 1
	private static int pages = 201;
	
	
	private static int limitePessoas = 60;
	private static int limiteGrupos = 5;
	
	private static String dataNascimento[] = {"12/02/1970",
											   "15/07/1987",
											   "29/09/1996",
											   "31/03/1988",
											   "01/11/1975",
											   "08/12/2000",
											   "29/05/1983",
											   "23/01/1992",
											   "02/16/1988",
											   "11/09/1980"};
	
	private static String dataRel[] = {"12/02/2006",
											"15/07/2010",
											"29/09/2009",
											"31/03/2008",
											"01/11/2011",
											"08/12/2012",
											"29/05/2007",
											"23/01/2010",
											"02/16/2011",
											"11/09/2011"};
	
	//ATENÇÃO QUANTO AO NUMERO DE PAGES
	private static int limiteInteresses = 30;
	
	//ATENÇÃO AO ALTERAR
	private static String [] tiposRelPages = new String[] {"gosta", "reside", "trabalha", "estuda"};

	public static void main(String[] args) throws SQLException {
		
		Integer[] listUsuarios = new Integer[usuarios];
		Integer[] listGrupos = new Integer[grupos];
		Integer[] listPages = new Integer[pages];
		PreparedStatement state, psSeq;
		Random generator = new Random();
		
		makeConnection();
		
		
		//insere usuarios
		System.out.println("criando usuários...");
		state = conn.prepareStatement("insert into usuarios values(?,?,?,?,?)");
		psSeq = conn.prepareStatement("select nextval('usuario_seq') ");
		int id = 0;
		
		for(int i=0;i<usuarios;i++){
			ResultSet rsSeq = psSeq.executeQuery();
			rsSeq.next();
			id = rsSeq.getInt(1);
			listUsuarios[i] = id;
			state.setInt(1, id);
			state.setString(2, "nome"+id);
			state.setString(3,dataNascimento[generator.nextInt(dataNascimento.length-1)]);
			state.setString(4, (id%2==0?"M":"F"));
			state.setString(5, "email"+id);
			state.execute();
			intermediateCommit();
			
		}
		state.close();
		psSeq.close();
			

		//insere grupos
		System.out.println("criando grupos...");
				state = conn.prepareStatement("insert into grupo values(?,?)");
				psSeq = conn.prepareStatement("select nextval('grupo_seq') ");
				
				for(int i=0;i<grupos;i++){
					ResultSet rsSeq = psSeq.executeQuery();
					rsSeq.next();
					id = rsSeq.getInt(1);
					listGrupos[i] = id;
					state.setInt(1, id);
					state.setString(2, "grupo"+id);
					state.execute();
					intermediateCommit();
					
				}
				state.close();
				psSeq.close();
				
				
				//insere paginas ATENÇÃO AO NUMERO DE PAGES DEFINIDA
				System.out.println("criando pages...");
				state = conn.prepareStatement("insert into page values(?,?,?)");
				int cont = 1;
					while (cont<=50){
						listPages[cont] = cont;
						state.setInt(1, cont);
						state.setString(2, "escola"+cont);
						state.setString(3, "escola");
						state.execute();
						intermediateCommit();
						cont++;
					}
					
					while(cont>50 && cont<=100){
						listPages[cont] = cont;
						state.setInt(1, cont);
						state.setString(2, "empresa"+cont);
						state.setString(3, "empresa");
						state.execute();
						intermediateCommit();
						cont++;	
					}
					
					while(cont>100 && cont<=150){
						listPages[cont] = cont;
						state.setInt(1, cont);
						state.setString(2, "cidade"+cont);
						state.setString(3, "cidade");
						state.execute();
						intermediateCommit();
						cont++;
					}
					
					while(cont>150 && cont<=200){
						listPages[cont] = cont;
						state.setInt(1, cont);
						state.setString(2, "interesse"+cont);
						state.setString(3, "interesse");
						state.execute();
						intermediateCommit();
						cont++;
					}
					state.close();
				
				
				
				//insere tipos de relacionamento com paginas
				state = conn.prepareStatement("insert into tipos_rel_page values(?,?)");	
				for(int i=0;i<tiposRelPages.length;i++){
					state.setInt(1, i);
					state.setString(2, tiposRelPages[i]);
					state.execute();
					intermediateCommit();
				}
				state.close();

				
				//insere tipos de relacionamento
				state = conn.prepareStatement("insert into tipos_rel_contato values(?,?)");
				state.setInt(1, 1);
				state.setString(2, "amigo");
				state.execute();
				state.setInt(1, 2);
				state.setString(2, "familia");
				state.execute();
				intermediateCommit();
			        state.close();


			        
			//relacionando contatos
			System.out.println("Relacionando pessoas...");
			state = conn.prepareStatement("insert into contatos values(?,?,?,?)");
			for(int i=0;i<usuarios;i++){
				Integer list[] = getIndexes(usuarios,limitePessoas,i);
				int dataRelac = generator.nextInt(dataRel.length-1);
			   	for(int k=0;k<6;k++){
					state.setInt(1,listUsuarios[i]);
					state.setInt(2,listUsuarios[list[k]]);
					state.setInt(3, 2);
					state.setString(4, dataRel[dataRelac]);
					state.execute();
					
					
				}
				for(int k=6;k<limitePessoas;k++){
					state.setInt(1,listUsuarios[i]);
					state.setInt(2,listUsuarios[list[k]]);
					state.setInt(3, 1);
					state.setString(4,  dataRel[dataRelac]);
					state.execute();
					
				}
				System.out.println(i);
			}	
			state.close();	
	



			//relacionando grupo
			System.out.println("Relacionando grupos...");
			state = conn.prepareStatement("insert into usuario_grupo values(?,?)");
			for(int i=0;i<usuarios;i++){
				Integer list[] = getIndexes(grupos,limiteGrupos,grupos+1);
			   	for(int k=0;k<limiteGrupos;k++){
					state.setInt(1,listUsuarios[i]);
					state.setInt(2,listGrupos[list[k]]);
					state.execute();
					intermediateCommit();
				}
				System.out.println("grupo"+i);
			}
			state.close();


		      //relacionando interesses,escolas e empresas
			System.out.println("Relacionando ESTUDA...");
			state = conn.prepareStatement("insert into usuario_page values(?,?,?)");
			int sort = 0;
			for(int i=0;i<usuarios;i++){
				while(sort == 0){
				   sort = generator.nextInt(50);
				}
				state.setInt(1,listUsuarios[i]);
				state.setInt(2,listPages[sort]);
				state.setInt(3,3);
				state.execute();
				intermediateCommit();
				sort = 0;
				System.out.println("escola"+i);
			}
			state.close();	
			
			System.out.println("Relacionando TRABALHA...");
			state = conn.prepareStatement("insert into usuario_page values(?,?,?)");
			sort = 0;
			for(int i=0;i<usuarios;i++){
				while(sort <= 50){
				   sort = generator.nextInt(100);
				}
				state.setInt(1,listUsuarios[i]);
				state.setInt(2,listPages[sort]);
				state.setInt(3,2);
				state.execute();	
				intermediateCommit();
				sort = 0;
				System.out.println("empresa"+i);
			}
			state.close();	
			
			System.out.println("Relacionando RESIDE...");
			state = conn.prepareStatement("insert into usuario_page values(?,?,?)");
			sort = 0;
			for(int i=0;i<usuarios;i++){
				while(sort <= 100){
				   sort = generator.nextInt(150);
				}
				state.setInt(1,listUsuarios[i]);
				state.setInt(2,listPages[sort]);
				state.setInt(3,1);
				state.execute();
				intermediateCommit();
				sort = 0;
				System.out.println("cidade"+i);
			}
			state.close();
			
			System.out.println("Relacionando INTERESSES...");
			state = conn.prepareStatement("insert into usuario_page values(?,?,?)");
			sort = 0;
			for(int i=0;i<usuarios;i++){
				Integer list[] = getIndexes(50,limiteInteresses,0);
				for(int k=0;k<limiteInteresses;k++){
					state.setInt(1,listUsuarios[i]);
					state.setInt(2,listPages[list[k]+150]);
					state.setInt(3,0);
					state.execute();
					intermediateCommit();
					sort = 0;
				}
					System.out.println("interesse"+i);
			}
			state.close();
			conn.close();

			

			System.out.println("CONCLUIDO em: "+new Date());
		
	}
	
	static void makeConnection() throws SQLException{
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost:5432/tcc";
				conn = DriverManager.getConnection(url, "postgres", "postgres");
			
		} catch (ClassNotFoundException e) {
			System.out.println("Erro no driver");
			}
		conn.setAutoCommit(false);
		}

      public static Integer[] getIndexes(int populacao, int limite, int individuo){
		Integer[] indexes = new Integer[limite];
		Random generator = new Random();
		
		for(int i=0;i<limite;i++){
			indexes[i] = 0;
		}
		
		for(int i=0; i<limite; i++){
			int existIndex = 0;
			int selectedIndex = generator.nextInt(populacao);
				for(int k=0;k<indexes.length;k++){
					if(indexes[k] == selectedIndex || selectedIndex == individuo){
						existIndex = 1;
						break;
					}
				}
			if(existIndex == 0){
				indexes[i] = selectedIndex; 
			}else{
				i--;
			}
		}
	    return indexes;
	}
      
      static int count = 0;

  	private static void intermediateCommit(){
  		if(count % 1000 == 0) {
  		   try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
  		}
  		count++;	
  	}
	
	
}
