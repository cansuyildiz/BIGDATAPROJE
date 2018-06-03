/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

/**
 *
 * @author cansu
 */
public class KMeans implements Serializable{
	
	public void run(String filePath, String inputs, JTextArea terminal) throws IOException, InterruptedException {
            
            class PrintThread implements Runnable{
                private String s;
                public PrintThread(String s){this.s = s;};
                @Override
                public void run() {
                    if (terminal != null){
                        terminal.setText(terminal.getText() + "\n" + s);
                        terminal.validate();
                    }                
                }
            };
            
            String s = null;
            try {
                
                //****************************
                // Get the input stream and read from it
                Process p = Runtime.getRuntime().exec("python3 " + filePath + inputs);           

                BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

                BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                // read the output from the command
                System.out.println("Here is the standard output of the command:\n");               
                while (p.isAlive()){
                    while ((s = stdInput.readLine()) != null || (s = stdError.readLine()) != null) {
                        System.out.println(s);
                        PrintThread pt = new PrintThread(s);
                        SwingUtilities.invokeLater(pt); 
                    }
                    Thread.sleep(50);
                }

                stdInput.close();
                stdError.close();

            }
            catch (IOException e) {
                System.out.println("exception happened - here's what I know: ");
                e.printStackTrace();
                System.exit(-1);
            }
        }
}






        


/*
            String s = null;
            try {
                
	    // run the Unix "ps -ef" command
            // using the Runtime exec method:
            Process p = Runtime.getRuntime().exec("python3 " + filePath);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            // read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            
            // read any errors from the attempted command
            System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);

            }
            
            //System.exit(0);
            }
            catch (IOException e) {
                System.out.println("exception happened - here's what I know: ");
                e.printStackTrace();
                System.exit(-1);
            }
*/
            
