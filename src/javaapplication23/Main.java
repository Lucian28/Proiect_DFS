import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.io.Console;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    
  static   String ANSI_BLUE = "\u001B[34m";
      static      String ANSI_RESET = "\u001B[0m";
     static       BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
         //dir();
     static  String fisierdel;
 static String str1;
        static      String name;
        static      String filename;
        static      int nr;
            // Client c = new Client();
        static      byte[] rez;
        static      byte[] ret;
        static      byte[] gol = null;
        static      boolean ok = true;
        static     String text;
       static String fisier;
        static           SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
              static String nume_fisier ;
        static           String newline = System.lineSeparator();
         static          char[] ss;
        static           byte[] data;
     static Date date;
    public static void dir(){
        File dir = new File("C:\\");
      File[] files = dir.listFiles();
      FileFilter fileFilter = new FileFilter() {
         public boolean accept(File file) {
            return file.isDirectory();
         }
      };
      files = dir.listFiles(fileFilter);
      System.out.println(files.length);
      
      if (files.length == 0) {
         System.out.println("Either dir does not exist or is not a directory");
      } else {
         for (int i = 0; i< files.length; i++) {
            File filename = files[i];
            System.out.println(filename.toString());
         }
      }
    }

	static int regPort = Configurations.REG_PORT;

	static Registry registry ;

	/**
	 * respawns replica servers and register replicas at master
	 * @param master
	 * @throws IOException
	 */
	static void respawnReplicaServers(Master master)throws IOException{
		System.out.println("[@main] respawning replica servers ");
		// TODO make file names global
		BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
		int n = Integer.parseInt(br.readLine().trim());
		ReplicaLoc replicaLoc;
		String s;

		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')) , true);
			ReplicaServer rs = new ReplicaServer(i, "./"); 

			ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
			registry.rebind("ReplicaClient"+i, stub);

			master.registerReplicaServer(replicaLoc, stub);

			System.out.println("replica server state [@ main] = "+rs.isAlive());
		}
		br.close();
	}

    public static void afisare_comenzi() {
        System.out.println(ANSI_BLUE + "Comenzile disponibile sunt:" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "dir" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "write" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "type" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "del" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Introduceti 'Exit' pentru a iesi" + ANSI_RESET);
        System.out.println();
    }

    public static int introducere_comanda() throws IOException {
        System.out.println(ANSI_BLUE + "Alege o comanda : 'dir', 'write', 'type' si 'del' " + ANSI_RESET);
        name = reader.readLine();
        
         if(name.equalsIgnoreCase("Exit"))
         return 0;
        if (name.equalsIgnoreCase("dir")) 
            return 1;
        if (name.equalsIgnoreCase("write")) 
            return 2;
        if (name.equalsIgnoreCase("type")) 
            return 3;
        
        if (name.equalsIgnoreCase("del")) 
            return 4;
        
        
        
        
         return 100;
    }

    public static void afisare_fisiere() {
        Client c3 = new Client(); // creare client nou 
        File directoryPath = new File("C:\\Users\\Pintea Sergiu\\Documents\\NetBeansProjects\\JavaApplication23\\Replica_0"); // retinem locatia directorului
        File directoryPathAux;
        String contents[] = directoryPath.list(); // tablou string cu numele tuturor fisierelor din folderul mare
 
        System.out.println(ANSI_BLUE + "Nr. fisiere : " + contents.length + ANSI_RESET); // ANSI_BLUE pentru culoarea asbastru
        System.out.println(ANSI_BLUE + "Fisierele si directoarele din folder: " + ANSI_RESET);
        for (int i = 0; i < contents.length; i++) {
            // urmeaza sa verificam care  este fisier si care e folder
            directoryPathAux=new File("C:\\Users\\Pintea Sergiu\\Documents\\NetBeansProjects\\JavaApplication23\\Replica_0\\"+contents[i]);
             // avem nevoie de un obiect de tip File pentru apelarea isFile() 
            if(directoryPathAux.isFile()==true)
             // daca e fisier
            System.out.println(contents[i]+" "
                    + "(fisier)");
            else // altfel e director
                 System.out.println(contents[i]+" (director)");
            
        }
      
    }

    public static void afisare_continut() throws IOException, NotBoundException, MessageNotFoundException {
        Client c = new Client();

        System.out.println(ANSI_BLUE + "Ati selectat comanda type: Introduceti numele fisierului pe care doriti sa-l cititi" + ANSI_RESET);
        System.out.println();
        fisier = reader.readLine(); // citim de la tastatura
        ss = " ".toCharArray(); // creem un fisier nou gol
        data = new byte[ss.length]; // pentru a putea citi un fisier fara a fi nevoie 
        for (int i = 0; i < ss.length; i++) { // sa il creem 
            data[i] = (byte) ss[i]; // nu e perfect.. dar functioneaza
        }
        c.write(fisier, data); // creem fisierul gol

        String str = new String(c.read(fisier)); // citim continutul fisierului
        date = new Date(System.currentTimeMillis()); //preluam data curenta
        text = newline + "Clientul " + c + " a citit fisierul: '" + fisier + "'" + " la data: " + formatter.format(date); //pregatim mesajul 
        ss = text.toCharArray(); // pentru fisierul log
        data = new byte[ss.length]; 
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c.write("loguri_type", data); // adaugam mesajul acolo
        System.out.println(ANSI_BLUE + "Continutul fisierului: :" + ANSI_RESET);
        System.out.println(str); // afisam pe consola continutul fisierului doritS

    }

    public static void stergere_fisier() throws IOException, NotBoundException, MessageNotFoundException {
        Client c2 = new Client();

        System.out.println(ANSI_BLUE + "Ati selectat comanda del: introduceti numele fisierului pe care doriti sa-l stergeti" + ANSI_RESET);
        System.out.println();
        fisierdel = reader.readLine(); // numele fisierului de sters
        nume_fisier = fisierdel;
        for (int x = 0; x < 3; x++) { // creem un for loop pentru a sterge din toate cele 3 replici fisierul dorit
            String fileName = "C:\\Users\\Pintea Sergiu\\Documents\\NetBeansProjects\\JavaApplication23\\Replica_" + x + "\\" + fisierdel;
            try {
                boolean exista = Files.deleteIfExists(Paths.get(fileName)); //sterge fisierul daca exista
                if(exista==true) // daca fisierul exista
                System.out.println(ANSI_BLUE + "Fisierul a fost sters cu succes" + ANSI_RESET);
                else // daca nu exista
                   System.out.println(ANSI_BLUE + "Fisierul nu a fost gasit" + ANSI_RESET); 

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        date = new Date(System.currentTimeMillis()); // inscriem in fisierul de log-uri actiunea
        text = newline + "Clientul " + c2.toString() + " a sters fisierul: '" + nume_fisier + "'" + " la data: " + formatter.format(date);
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c2.write("loguri_delete", data);
    }

    public static void scrie_in_fisier() throws IOException, NotBoundException, MessageNotFoundException {
        Client c1 = new Client();
        System.out.println(ANSI_BLUE + "Ati selectat comanda write: introduceti numele fisierului in care doriti sa scrieti" + ANSI_RESET);
        System.out.println();
        filename = reader.readLine();
        System.out.println(ANSI_BLUE + "Introduceti textul: " + ANSI_RESET);
        System.out.println();
        text = reader.readLine();
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c1.write(filename, data);

        date = new Date(System.currentTimeMillis());
        text = newline + "Clientul " + c1 + " a creat fisierul: '" + filename + "'" + " la data: " + formatter.format(date);
        ss = text.toCharArray();
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }
        c1.write("loguri_write", data);
        ;
    }
         
	   public static void launchClients() {
        try {

            while (ok == true) {

                nr = introducere_comanda();
                switch (nr) {
                    case 100:
                        System.out.println("Ai introdus o comanda gresita");
                        break;
                    // afisarea fisierelor dintr-un director  -----------------------------------------------------------------------------------------------------------------------------------------
                    case 1:

                        afisare_fisiere();
                        break;

                    case 2: // afisarea continutului unui fisier  -----------------------------------------------------------------------------------------------------------------------------------------
                        scrie_in_fisier();

                        break;

                    case 3: // stergerea unui fisier -----------------------------------------------------------------------------------------------------------------------------------------
                        afisare_continut();

                        break;

                    case 4:  // scrierea intr-un fisier  -----------------------------------------------------------------------------------------------------------------------------------------

                        stergere_fisier();
                        break;

                        
                    case 0:

                        System.out.println(ANSI_BLUE + "EXIT" + ANSI_RESET);
                        System.exit(0);
                }
            }


        } catch (NotBoundException | IOException | MessageNotFoundException e) {
            e.printStackTrace();
        }
    }
//  ----------------------------------------------------------------------------------------------------------------------------------------- -----------------------------------------------------------------------------------------------------------------------------------------
	/**
	 * runs a custom test as follows
	 * 1. write initial text to "file1"
	 * 2. reads the recently text written to "file1"
	 * 3. writes a new message to "file1"
	 * 4. while the writing operation in progress read the content of "file1"
	 * 5. the read content should be = to the initial message
	 * 6. commit the 2nd write operation
	 * 7. read the content of "file1", should be = initial messages then second message 
	 * 
	 * @throws IOException
	 * @throws NotBoundException
	 * @throws MessageNotFoundException
	 */
	public  static void customTest() throws IOException, NotBoundException, MessageNotFoundException{
		Client c = new Client();
		String fileName = "file1";

		char[] ss = "[INITIAL DATA!]".toCharArray(); // len = 15
		byte[] data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		c.write(fileName, data);

		c = new Client();
		ss = "File 1 test test END".toCharArray(); // len = 20
		data = new byte[ss.length];
		for (int i = 0; i < ss.length; i++) 
			data[i] = (byte) ss[i];

		
		byte[] chunk = new byte[Configurations.CHUNK_SIZE];

		int seqN =data.length/Configurations.CHUNK_SIZE;
		int lastChunkLen = Configurations.CHUNK_SIZE;

		if (data.length%Configurations.CHUNK_SIZE > 0) {
			lastChunkLen = data.length%Configurations.CHUNK_SIZE;
			seqN++;
		}
		
		WriteAck ackMsg = c.masterStub.write(fileName);
		ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());

		FileContent fileContent;
		@SuppressWarnings("unused")
		ChunkAck chunkAck;
		//		for (int i = 0; i < seqN; i++) {
		System.arraycopy(data, 0*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);


		System.arraycopy(data, 1*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

		// read here 
		List<ReplicaLoc> locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read1 started ");

		// TODO fetch from all and verify 
		ReplicaLoc replicaLoc = locations.get(0);
		ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));


		// continue write 
		for(int i = 2; i < seqN-1; i++){
			System.arraycopy(data, i*Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
			fileContent = new FileContent(fileName, chunk);
			chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
		}
		// copy the last chuck that might be < CHUNK_SIZE
		System.arraycopy(data, (seqN-1)*Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
		fileContent = new FileContent(fileName, chunk);
		chunkAck = stub.write(ackMsg.getTransactionId(), seqN-1, fileContent);

		
		
		//commit
		ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(ackMsg.getTransactionId(), seqN);

		
		// read
		locations = c.masterStub.read(fileName);
		System.err.println("[@CustomTest] Read3 started ");

		replicaLoc = locations.get(0);
		replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		fileContent = replicaStub.read(fileName);
		System.err.println("[@CustomTest] data:");
		System.err.println(new String(fileContent.getData()));

	}

	static Master startMaster() throws AccessException, RemoteException{
		Master master = new Master();
		MasterServerClientInterface stub = 
				(MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
		registry.rebind("MasterServerClientInterface", stub);
		System.err.println("Server ready");
		return master;
	}

	public static void main(String[] args) throws IOException {


		try {
			LocateRegistry.createRegistry(regPort);
			registry = LocateRegistry.getRegistry(regPort);

			Master master = startMaster();
			respawnReplicaServers(master);

//			customTest();
                        afisare_comenzi();
			launchClients();
                        

		} catch (RemoteException   e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}

}
