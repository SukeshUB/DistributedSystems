package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] portArr = new String[]{"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    static final String ACK = "ACKNOWLEDGED";
    static int sequenceNumber =0;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static int sequenceInProcess =0;
    static int messageSequence =0;
    static List<MessageObject> holdBackQueue = new ArrayList<MessageObject>();
    static String myPortNumber ;
    static Map<String,PriorityQueue<Pair>> map = new HashMap<String,PriorityQueue<Pair>>();
    static boolean isSent = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        //Log.i(TAG,"Port is"+myPort);
        myPortNumber = myPort;

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        final EditText text = (EditText) findViewById(R.id.editText1);
        final View button = (Button) findViewById(R.id.button4);
        button.setOnClickListener(new View.OnClickListener(){

            public void onClick(View v) {
                String msg = text.getText().toString();
                text.setText(""); // This is one way to reset the input box.
                //Log.i(TAG,"Message in onclick is" + msg);
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true) {
                try {
                    Socket server = serverSocket.accept();
                    //Log.i(TAG, "Connection from Server established to" + server.getRemoteSocketAddress());
                    InputStream inputFromClient = server.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputFromClient));
                    //Log.i(TAG, "After bufferedreader");
                    String input = "";
                    if((input=in.readLine())!=null)
                    {
                        //Log.i(TAG, input);
                        String[] tokens = input.split("\\|");
                        if(tokens.length==3){
                            //Log.i(TAG, "3 Tokens");
                            sequenceInProcess++;
                            //Log.i("Counter Progress",sequenceInProcess+ "Sequence is" +tokens[2]);
                            holdBackQueue.add(new MessageObject(tokens[0],tokens[1],tokens[2],sequenceInProcess,myPortNumber,false));
                            Collections.sort(holdBackQueue,new myComp());
                            OutputStream outToClient = server.getOutputStream();
                            //Log.i(TAG,"From Server output stream");
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =tokens[1]+"|"+sequenceInProcess+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }else if(tokens.length==4){
                            Log.i(TAG, "4 Tokens");
                            sequenceInProcess=Math.max(sequenceInProcess,Integer.parseInt(tokens[2]));
                            changeSequenceValue(holdBackQueue,tokens[0],tokens[1],tokens[2],tokens[3]);
                            Collections.sort(holdBackQueue,new myComp());

                            //Log.i(TAG,"Head msg is"+holdBackQueue.get(0).message);
                            //Log.i(TAG,"Can it be delivered"+holdBackQueue.get(0).canbeDelivered);
                            while(holdBackQueue.size()>0 && holdBackQueue.get(0).canbeDelivered){
                                //Log.i(TAG,"In Delivery");
                                MessageObject mobj = holdBackQueue.remove(0);
                               // Log.i(TAG,"Delivery Msg"+mobj.message);
                                String msgToBeInserted = mobj.message;
                                Log.i("Agreed Sequence","Seq --"+ mobj.suggestedSequence+ "Orig Port"+mobj.messageSentProcess+"Msg"+msgToBeInserted);
                                ContentResolver cr =getContentResolver();
                                Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
                                ContentValues cv = new ContentValues();
                                cv.put(KEY_FIELD,String.valueOf(sequenceNumber));
                                sequenceNumber++;
                                cv.put(VALUE_FIELD,msgToBeInserted);
                                cr.insert(uri,cv);
                                String msgToDeliver =msgToBeInserted+" seq no : "+sequenceNumber;
                                publishProgress(msgToDeliver);
                            }

                            OutputStream outToClient = server.getOutputStream();
                           // Log.i(TAG,"From Server output stream");
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK;
                            out.writeBytes(dataToSend);
                            out.flush();

                            //Log.i(TAG,"Server closing");
                            server.close();
                            //Log.i(TAG,"Server closed");

                        }else if(tokens.length==1){
                            deleteMessages(holdBackQueue,tokens[0]);
                            //Log.i(TAG,"Server closing");
                            server.close();
                            //Log.i(TAG,"Server closed");

                        }
                    }


                } catch (IOException e) {
                    Log.e(TAG, "Server side exception");
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            //Log.i(TAG,"In Progress Update");
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            //Log.i(TAG, ""+R.id.textView1);
            remoteTextView.append(strReceived +"\t\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            messageSequence++;
            try {
                for(int i=0;i<portArr.length;i++){
                    String remotePort = portArr[i];
                    String msg = msgs[0];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String toSendMsg =msg+"|"+messageSequence+"|"+msgs[1]+"\n";
                    //Log.i(TAG,"Message to be written"+remotePort+toSendMsg);
                    out.writeBytes(toSendMsg);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    try{
                        String msgFromClient =in.readLine();
                        //Log.i(TAG,"Before vals"+msgFromClient);
                        String[] vals = msgFromClient.split("\\|");
                        //Log.i(TAG,"values"+vals.length+"-----"+vals[0]+"--"+vals[1]);
                        if(!map.containsKey(vals[0])){
                            PriorityQueue<Pair> pairPriorityQueue = new PriorityQueue<Pair>(10,new PairComparatorImpl());
                            pairPriorityQueue.add(new Pair(vals[1],remotePort));
                            map.put(vals[0],pairPriorityQueue);
                        }else {
                            map.get(vals[0]).add(new Pair(vals[1],remotePort));
                        }

                    }catch(NullPointerException n){
                        if(!isSent) {
                            isSent = true;
                            Log.i("Null-Po-1st-cast", "Nul");
                            for (int k = 0; k < portArr.length; k++) {
                                if (portArr[k] == remotePort) continue;
                                String newPort = portArr[k];
                                Socket socketF = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(newPort));
                                OutputStream inputToServerF = socketF.getOutputStream();
                                DataOutputStream outF = new DataOutputStream(inputToServerF);
                                String toSendMsgF = remotePort + "\n";
                                //Log.i(TAG,"Message to be written"+remotePort+toSendMsg);
                                outF.writeBytes(toSendMsgF);
                                outF.flush();
                            }
                        }
                    }
                }

                //Second Multicast
                Pair agreedpair = map.get(String.valueOf(messageSequence)).poll();
                for(int i=0;i<portArr.length;i++){
                    Log.i(TAG,"Second Multicast");
                    String remotePort = portArr[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String dataToSend =messageSequence+"|"+msgs[1]+"|"+agreedpair.suggestedSequenceNumber+"|"+agreedpair.suggestedPort+"\n";
                    out.writeBytes(dataToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    try{
                        String msgFromClient =in.readLine();
                        if(msgFromClient.equals(ACK)){
                            socket.close();
                        }
                    }catch(NullPointerException n) {
                        Log.i("Null-Po-1st-cast", "Nul");
                        if (!isSent){
                            String failedPort = remotePort;
                            isSent=true;
                            for (int k = 0; k < portArr.length; k++) {
                                if (portArr[k] == failedPort) continue;
                                String newPort = portArr[k];
                                Socket socketF = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(newPort));
                                OutputStream inputToServerF = socketF.getOutputStream();
                                DataOutputStream outF = new DataOutputStream(inputToServerF);
                                String toSendMsgF = failedPort + "\n";
                                //Log.i(TAG,"Message to be written"+remotePort+toSendMsg);
                                outF.writeBytes(toSendMsgF);
                                outF.flush();
                            }
                    }
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            } catch(NullPointerException n){
                Log.e(TAG, "ClientTask socket NUll Exception");
            }

            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public void changeSequenceValue(List<MessageObject> list , String messageId,String messageSentProcess,String acceptedSequence,String accSeqSentProc){
        int index=-1;
        for(int i=0;i<list.size();i++){

            if(list.get(i).messageId.equals(messageId) && list.get(i).messageSentProcess.equals(messageSentProcess)){
                index=i;
                break;
            }


        }
        if(index!=-1) {
            list.get(index).suggestedSequence = Integer.parseInt(acceptedSequence);
            list.get(index).sequenceSuggestedProcess = accSeqSentProc;
            list.get(index).canbeDelivered = true;
        }
    }

    public void deleteMessages(List<MessageObject> list, String port){

        Iterator<MessageObject> iter = list.iterator();
        while(iter.hasNext()){
            MessageObject object = iter.next();
            if(object.messageSentProcess.equals(port) && !object.canbeDelivered)
                iter.remove();
        }



    }

}

class MessageObject {

    String message;
    String messageId;
    String messageSentProcess;
    int suggestedSequence;
    String sequenceSuggestedProcess;
    boolean canbeDelivered;

    public MessageObject(String message,String messageId,String messageSentProcess,int suggestedSequence,String sequenceSuggestedProcess,boolean canbeDelivered){

        this.message=message;
        this.messageId=messageId;
        this.messageSentProcess=messageSentProcess;
        this.sequenceSuggestedProcess=sequenceSuggestedProcess;
        this.suggestedSequence=suggestedSequence;
        this.canbeDelivered=canbeDelivered;

    }
}

class Pair{

    String suggestedSequenceNumber;
    String suggestedPort;

    public Pair(String suggestedSequenceNumber,String suggestedPort){

        this.suggestedSequenceNumber=suggestedSequenceNumber;
        this.suggestedPort=suggestedPort;
    }
}

class PairComparatorImpl implements Comparator<Pair>{

    @Override
    public int compare(Pair p1,Pair p2){

        if(p1.suggestedSequenceNumber.equals(p2.suggestedSequenceNumber)){
            return Integer.parseInt(p1.suggestedPort)-Integer.parseInt(p2.suggestedPort);
        }else{
            return Integer.parseInt(p2.suggestedSequenceNumber)-Integer.parseInt(p1.suggestedSequenceNumber);
        }
    }
}

class myComp implements Comparator<MessageObject>{

    @Override
    public int compare(MessageObject obj1,MessageObject obj2){
        if(obj1.suggestedSequence==obj2.suggestedSequence){
            if((obj1.canbeDelivered && !obj2.canbeDelivered) || (!obj1.canbeDelivered && obj2.canbeDelivered)){
                return Boolean.valueOf(obj1.canbeDelivered).compareTo(Boolean.valueOf(obj2.canbeDelivered));
            }else {
                return Integer.parseInt(obj1.sequenceSuggestedProcess)-Integer.parseInt(obj2.sequenceSuggestedProcess);
            }
        }else {
            return obj1.suggestedSequence-obj2.suggestedSequence;
        }
    }

}


