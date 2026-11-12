

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;

public class DataServer
{
	public static void main(String[] args) throws IOException
	{
		ServerSocket listener = new ServerSocket(9090);
		try{
				Socket socket = listener.accept();
				System.out.println("Got new connection: " + socket.toString());
				
				BufferedReader br = new BufferedReader(new FileReader(args.length > 0 ? args[0] : "avg.txt"));
				
				try {
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					String line;
					int count = 0;
					while ((line = br.readLine()) != null){
						count++;
						
						out.println(line);
						if (count >= 10){
							count = 0;
							Thread.sleep(2000);
						}
						else
							Thread.sleep(50);
					}
					
				} finally{
					socket.close();
				}
			
		} catch(Exception e ){
			e.printStackTrace();
		} finally{
			
			listener.close();
		}
	}
}

