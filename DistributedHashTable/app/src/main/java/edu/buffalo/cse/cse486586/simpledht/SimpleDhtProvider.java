package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String[] portArr = new String[]{"11108","11112","11116","11120","11124"};
    //String[] hashes = new String[portArr.length];
    static String myPortNumber ;
    static String myEmulNumber;
    static String myHash;
    static String predHash;
    static String succHash;
    static String predPort;
    static String succPort;
    static String highestHash ="";
    static String lowestHash="";
    static String lowestHashPort;
    static Map<String,String> hashPort = new HashMap<String,String>();
    static final String[] emulArr = new String[]{"5554","5556","5558","5560","5562"};
    static final int SERVER_PORT = 10000;
    static final String ACK = "ACKNOWLEDGED";
    static List<String> activePorts = new ArrayList<String>();
    static boolean isHighKey = false;
    static String starfrom =null;
    static String starDeletefrom =null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String filename = selection;
        if(succHash ==null && filename.equals("*")){
            String[] files =  getContext().fileList();
            for(String file : files){
                getContext().deleteFile(file);
            }
        }else if(filename.equals("@")){
            String[] files =  getContext().fileList();
            for(String file : files){
                getContext().deleteFile(file);
            }
        }else if(succHash==null){
            getContext().deleteFile(filename);
        }else if(succHash!=null && filename.equals("*") && starDeletefrom ==null){
            String[] files =  getContext().fileList();
            for(String file : files){
                getContext().deleteFile(file);
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPortNumber,"SFD");
        }else if(succHash!=null && filename.equals("*") && starDeletefrom!=null){
            String[] files =  getContext().fileList();
            for(String file : files){
                getContext().deleteFile(file);
            }
            if(!starDeletefrom.equals(succPort))
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, starDeletefrom,"SFD");
        }
        else if(isHighKey){
            Log.i("High key is true",isHighKey+"");
            isHighKey=false;
            getContext().deleteFile(filename);
        }
        else{

            String keyHash = "";
            try {
                keyHash = genHash(filename);
            } catch (NoSuchAlgorithmException n) {
                Log.i("Insert Key Hash", "Generate key hash exception");
            }
            if(keyHash.compareTo(highestHash) >0 && keyHash.compareTo(lowestHash) > 0){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename,"FFD");
            }
            else if(keyHash.compareTo(highestHash)<0 && keyHash.compareTo(lowestHash) <0){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename,"FFD");
            }
            else if (keyHash.compareTo(myHash) < 0 && keyHash.compareTo(predHash) > 0) {
                getContext().deleteFile(filename);
            } else {
                Log.i("Pass on",keyHash);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename,"PFD");
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String filename = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);

        if(succHash==null){
            Log.i("Succ is Null",succHash+"");
            Log.i(TAG,"Inserting"+filename+"---"+value);
            FileOutputStream outputStream;

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed"+e);
            }
        }
        else if(isHighKey){
            Log.i("High key is true",isHighKey+"");
            isHighKey=false;
            Log.i(TAG, "Inserting" + filename + "---" + value);
            FileOutputStream outputStream;

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed" + e);
            }

        }
        else {
            String keyHash = "";
            try {
                keyHash = genHash(filename);
                Log.i("Keyhash is",keyHash);
            } catch (NoSuchAlgorithmException n) {
                Log.i("Insert Key Hash", "Generate key hash exception");
            }
            Log.i("Keyhash",keyHash);
            Log.i("Highhash",highestHash);
            Log.i("Lowhash",lowestHash);
            if(keyHash.compareTo(highestHash) >0 && keyHash.compareTo(lowestHash) > 0){
                Log.i("The highest",keyHash);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value,"HH");
            }
            else if(keyHash.compareTo(highestHash)<0 && keyHash.compareTo(lowestHash) <0){
                Log.i("Lowest",keyHash);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value,"HH");
            }
            else if (keyHash.compareTo(myHash) < 0 && keyHash.compareTo(predHash) > 0) {
                Log.i("In range",keyHash);
                Log.i("Node is", "My node");
                Log.i(TAG, "Inserting" + filename + "---" + value);
                FileOutputStream outputStream;

                try {
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "File write failed" + e);
                }

            } else {
                Log.i("Pass on",keyHash);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value);
            }
            Log.v("insert", values.toString());
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
       // TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));
        myEmulNumber = String.valueOf(Integer.parseInt(portStr));
        Log.i("OnCreate",myPortNumber);

        succPort=null;
        succHash=null;
        predPort=null;
        predHash=null;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if(!myPortNumber.equals("11108"))
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPortNumber,"Conn");
            else {

                activePorts.add(myPortNumber);

            }
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        // TODO Auto-generated method stub
        return false;
    }

    public void assignSuccPred(){
        List<String> hashes = new ArrayList<String>();
        for(int i=0;i<activePorts.size();i++){
            try {
                int value = Integer.parseInt(activePorts.get(i));
                value = value/2;
                String portHash = String.valueOf(value);
                String hashValue = genHash(portHash);
                hashes.add(hashValue);
                hashPort.put(hashValue,activePorts.get(i));
                if(activePorts.get(i).equals(myPortNumber))
                    myHash=hashValue;
            }catch (NoSuchAlgorithmException n){
                Log.i("Has","Hash generation exception");
            }
        }
        Collections.sort(hashes);
        for(int i=0;i<hashes.size();i++){

            if(hashes.get(i).equals(myHash)) {
                predHash = i == 0 ? hashes.get(hashes.size()- 1) : hashes.get(i - 1);
                succHash = i == hashes.size() - 1 ? hashes.get(0) : hashes.get(i + 1);
                predPort = hashPort.get(predHash);
                succPort = hashPort.get(succHash);
            }
            if(hashes.get(i).compareTo(highestHash) > 0){
                    highestHash = hashes.get(i);
            }
            Log.i("HashPointers",myHash+"--"+predHash+"--"+succHash);
            Log.i("PortPointers",myPortNumber+"--"+predPort+"--"+succPort);
        }

        lowestHash = highestHash;
        for(int i=0;i<hashes.size();i++){

            if(hashes.get(i).compareTo(lowestHash)<0){
                lowestHash = hashes.get(i);
            }
        }
        Log.i("HighestHash",highestHash);
        Log.i("LowestHash",lowestHash);
        lowestHashPort=hashPort.get(lowestHash);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        String filename = selection;
        FileInputStream inputStream;
        String result = "";
        if(succHash ==null && filename.equals("*")){
                String[] files =  getContext().fileList();
                Log.i("Files", files.length+"" +files[0]+"--"+files[1]);
                String[] columns = new String[2];
                columns[0] = KEY_FIELD;
                columns[1] = VALUE_FIELD;
                MatrixCursor mc = new MatrixCursor(columns);
               for(String file : files){

                   try {
                       inputStream = getContext().openFileInput(file);
                       byte[] input = new byte[inputStream.available()];
                       while (inputStream.read(input) != -1) {
                           result += new String(input);
                       }
                       inputStream.close();
                   } catch (Exception e) {
                       Log.e(TAG, "Query -- File write failed" + e);
                   }

                   String[] values = new String[2];
                   values[0] = file;
                   values[1] = result;
                   mc.addRow(values);
                   result="";
                   Log.v("query", selection);
               }
            return mc;

        }else if(filename.equals("@")){
            String[] files =  getContext().fileList();
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            MatrixCursor mc = new MatrixCursor(columns);
            for(String file : files){

                try {
                    inputStream = getContext().openFileInput(file);
                    byte[] input = new byte[inputStream.available()];
                    while (inputStream.read(input) != -1) {
                        result += new String(input);
                    }
                    inputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "Query -- File write failed" + e);
                }

                String[] values = new String[2];
                values[0] = file;
                values[1] = result;
                mc.addRow(values);
                result="";
                Log.v("query", selection);
            }
            return mc;
        }
        else if(succHash==null){
            try {
                inputStream = getContext().openFileInput(filename);
                byte[] input = new byte[inputStream.available()];
                while (inputStream.read(input) != -1) {
                    result += new String(input);
                }
                inputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "Query -- File write failed" + e);
            }
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            String[] values = new String[2];
            values[0] = filename;
            values[1] = result;
            MatrixCursor mc = new MatrixCursor(columns);
            mc.addRow(values);
            Log.v("query", selection);
            return mc;
        }
        else if(succHash!=null && filename.equals("*") && starfrom==null){
            Log.i("First Star",myPortNumber);
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            MatrixCursor mc = new MatrixCursor(columns);
            try {
                String querystarres = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPortNumber,"SQ").get();
                if(querystarres.length()>0) {
                    String[] vals = querystarres.split(",");
                    for (String value : vals) {
                        String[] keyval = value.split("#");
                        Log.i("keyval", "" + keyval.length);
                        for (int i = 0; i < keyval.length; i++) {
                            Log.i("Keyval is", keyval[i]);
                        }
                        mc.addRow(keyval);
                    }
                }
                String[] files =  getContext().fileList();
                for(String file : files){
                    try {
                        inputStream = getContext().openFileInput(file);
                        byte[] input = new byte[inputStream.available()];
                        while (inputStream.read(input) != -1) {
                            result += new String(input);
                        }
                        inputStream.close();
                    } catch (Exception e) {
                        Log.e(TAG, "Query -- File write failed" + e);
                    }
                    String[] values = new String[2];
                    values[0] = file;
                    values[1] = result;
                    mc.addRow(values);
                    result="";
                    Log.v("query", selection);
                }
            }catch (InterruptedException i){
                Log.i("IEStar","IEStar");
            }catch (ExecutionException e){
                Log.i("EException","EE");
            }
            return mc;
        }
        else if(succHash!=null && filename.equals("*") && starfrom!=null){
            Log.i("Star on",myPortNumber+"--"+starfrom);
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            MatrixCursor mc = new MatrixCursor(columns);

            if(!starfrom.equals(succPort)){
                Log.i("Star--succHash",succHash);
                try{
                    String querystarres = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, starfrom,"SQ").get();
                    if(querystarres.length()>0) {
                        String[] vals = querystarres.split(",");
                        for (String value : vals) {
                            Log.i("Value is",value);
                            String[] keyval = value.split("#");
                            for (int i = 0; i < keyval.length; i++) {
                                Log.i("Keyval", keyval[i]);
                            }
                            mc.addRow(keyval);
                        }
                    }
                }
                catch (InterruptedException i){
                    Log.i("IEStar","IEStar");
                }catch (ExecutionException e){
                    Log.i("EException","EE");
                }
                Log.i("End of next star","Star");
            }
            Log.i("Outside SuccHash",succHash);
            String[] files =  getContext().fileList();
            Log.i("FIles size",""+files.length);
            for(String file : files){
                try {
                    inputStream = getContext().openFileInput(file);
                    byte[] input = new byte[inputStream.available()];
                    while (inputStream.read(input) != -1) {
                        result += new String(input);
                    }
                    inputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "Query -- File write failed" + e);
                }
                String[] values = new String[2];
                values[0] = file;
                values[1] = result;
                mc.addRow(values);
                result="";
                Log.v("querystar", selection);
            }
            Log.i("Before mc return","before mc return");
            return mc;
        }
        else if(isHighKey){
            isHighKey =false;
            try {
                inputStream = getContext().openFileInput(filename);
                byte[] input = new byte[inputStream.available()];
                while (inputStream.read(input) != -1) {
                    result += new String(input);
                }
                inputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "Query -- File write failed" + e);
            }
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            String[] values = new String[2];
            values[0] = filename;
            values[1] = result;
            MatrixCursor mc = new MatrixCursor(columns);
            mc.addRow(values);
            Log.v("query", selection);
            return mc;
        }
        else {
            String hashKey ="";
            try{
                hashKey = genHash(filename);
            }catch(NoSuchAlgorithmException n){
                Log.i("Selection hash","Selection");
            }

            if(hashKey.compareTo(highestHash) >0 && hashKey.compareTo(lowestHash) > 0){
                Log.i("The highest",hashKey);
                try {
                    String queryOut = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QH").get();
                    String[] columns = new String[2];
                    columns[0] = KEY_FIELD;
                    columns[1] = VALUE_FIELD;
                    String[] values = new String[2];
                    values[0] = filename;
                    values[1] = queryOut;
                    MatrixCursor mc = new MatrixCursor(columns);
                    mc.addRow(values);
                    Log.v("query", filename);
                    return mc;
                }catch(InterruptedException i){
                    Log.i("Interrupted Exception","IE");
                }catch(ExecutionException e){
                    Log.i("Execution Exception","EE");
                }
            }
            else if(hashKey.compareTo(highestHash)<0 && hashKey.compareTo(lowestHash) <0){
                Log.i("The highest",hashKey);
                try {
                    String queryOut = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QH").get();
                    String[] columns = new String[2];
                    columns[0] = KEY_FIELD;
                    columns[1] = VALUE_FIELD;
                    String[] values = new String[2];
                    values[0] = filename;
                    values[1] = queryOut;
                    MatrixCursor mc = new MatrixCursor(columns);
                    mc.addRow(values);
                    Log.v("query", filename);
                    return mc;
                }catch(InterruptedException i){
                    Log.i("Interrupted Exception","IE");
                }catch(ExecutionException e){
                    Log.i("Execution Exception","EE");
                }
            }
            else if (hashKey.compareTo(myHash) < 0 && hashKey.compareTo(predHash) > 0) {
                try {
                    inputStream = getContext().openFileInput(filename);
                    byte[] input = new byte[inputStream.available()];
                    while (inputStream.read(input) != -1) {
                        result += new String(input);
                    }
                    inputStream.close();
                } catch (Exception e) {
                    Log.e(TAG, "Query -- File write failed" + e);
                }
                String[] columns = new String[2];
                columns[0] = KEY_FIELD;
                columns[1] = VALUE_FIELD;
                String[] values = new String[2];
                values[0] = filename;
                values[1] = result;
                MatrixCursor mc = new MatrixCursor(columns);
                mc.addRow(values);
                Log.v("query", selection);
                return mc;

            } else {
                Log.i("Pass on",hashKey);
                try {
                    String queryOut = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, "QS").get();
                    String[] columns = new String[2];
                    columns[0] = KEY_FIELD;
                    columns[1] = VALUE_FIELD;
                    String[] values = new String[2];
                    values[0] = filename;
                    values[1] = queryOut;
                    MatrixCursor mc = new MatrixCursor(columns);
                    mc.addRow(values);
                    Log.v("query", selection);
                    return mc;
                }catch(InterruptedException i){
                    Log.i("Interrupted Exception","IE");
                }catch(ExecutionException e){
                    Log.i("Execution Exception","EE");
                }
            }
            Log.v("query", hashKey);
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true) {
                try {
                    //Log.i(TAG,"Before Server connection established");
                    Socket server = serverSocket.accept();
                    //Log.i(TAG, "Connection from Server established to" + server.getRemoteSocketAddress());
                    InputStream inputFromClient = server.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputFromClient));
                    Log.i("InServerIn","Hai");
                    String input = "";

                    if((input=in.readLine())!=null)
                    {
                        String[] msgs = input.split(",");
                        Log.i("Message Length",msgs.length+"--");
                        if(msgs[1].equals("Connection")){
                            Log.i("Connection","Request sent by"+msgs[0]);
                            activePorts.add(msgs[0]);
                            assignSuccPred();
                            OutputStream outToClient = server.getOutputStream();
                            // Log.i(TAG,"From Server output stream");
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            publishProgress(msgs[0]);

                        }else if(msgs[0].equals("New")){
                            Log.i("NewServer",msgs.length+"--");
                            for(String ap : msgs){
                                if(!ap.equals("New"))
                                    activePorts.add(ap);
                            }
                            assignSuccPred();
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }else if(msgs[0].equals("Old")){
                            Log.i("OldServer","Node joined is"+msgs[1]);
                            activePorts.add(msgs[1]);
                            assignSuccPred();
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();

                        }
                        else if(msgs[1].equals("QH")){
                            isHighKey=true;
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            Cursor rc = query(uri,null,msgs[0],null,null);
                            int keyIndex = rc.getColumnIndex(KEY_FIELD);
                            int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                            rc.moveToFirst();
                            String returnValue = rc.getString(valueIndex);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =returnValue+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else if(msgs[1].equals("QS")){
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            Cursor rc = query(uri,null,msgs[0],null,null);
                            int keyIndex = rc.getColumnIndex(KEY_FIELD);
                            int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                            rc.moveToFirst();
                            String returnValue = rc.getString(valueIndex);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =returnValue+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else if(msgs[1].equals("SQ")){
                            starfrom =msgs[0];
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            Cursor rc = query(uri,null,"*",null,null);
                            StringBuffer sb = new StringBuffer();
                            Log.i("Rc count",""+rc.getCount());
                            String resultStar ="";
                            if(rc.getCount()>0){
                                while(rc.moveToNext()){
                                    Log.i("In Cursor iterator",rc.toString());
                                    int keyIndex = rc.getColumnIndex(KEY_FIELD);
                                    int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                                    String returnKey = rc.getString(keyIndex);
                                    String returnValue = rc.getString(valueIndex);
                                    sb.append(returnKey).append("#").append(returnValue).append(",");
                                }
                                sb.deleteCharAt(sb.length()-1);
                                resultStar = sb.toString();
                            }
                            Log.i("Data",sb.toString()+"---"+myPortNumber);
                            starfrom=null;
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =resultStar+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else if(msgs[1].equals("FFD")){
                            isHighKey=true;
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            delete(uri,msgs[0],null);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else if(msgs[1].equals("PFD")){

                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            delete(uri,msgs[0],null);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else if(msgs[1].equals("SFD")){
                            starDeletefrom=msgs[0];
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            delete(uri,"*",null);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                        else{
                            Log.i("FinalElseServer",isHighKey+"--isHighKey");
                            if(msgs.length > 2 && msgs[2].equals("true")) isHighKey =true;
                            ContentResolver cr = getContext().getContentResolver();
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD,msgs[0]);
                            cv.put(VALUE_FIELD,msgs[1]);
                            cr.insert(uri,cv);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend =ACK+"\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                    }

                    server.close();

                } catch (IOException e) {
                    Log.e(TAG, "Server side exception");
                }
            }
        }

        protected void onProgressUpdate(String...strings) {

            for(int i=0;i<activePorts.size();i++){
                Log.i("InOnProgress",activePorts.get(i)+"--Port");
                if(activePorts.get(i).equals("11108"))
                    continue;
                else if(activePorts.get(i).equals(strings[0])){
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,strings[0],"New");
                }else{
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,strings[0],"Old",activePorts.get(i));
                }
            }
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            Log.i("Msg[1]",msgs[1]);
            if (msgs[1].equals("Conn")) {
                try {
                    Log.i("Connection-Client",msgs[0]+"--Port is");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0]+","+"Connection"+"\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch(NullPointerException n){
                    Log.i("Single Em","Single Em");
                }
            }
            else if(msgs[1].equals("New")){
                try {
                    Log.i("NewClient",msgs[0]+"--New Node joined");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    StringBuilder msgToSend = new StringBuilder();
                    for(int i=0;i<activePorts.size();i++){
                        msgToSend.append(activePorts.get(i)).append(",");
                    }
                    msgToSend.deleteCharAt(msgToSend.length()-1);
                    String toSend = "New"+","+msgToSend.toString()+"\n";
                    out.writeBytes(toSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }else if(msgs[1].equals("Old")){
                try{
                        Log.i("OldClient",msgs[2]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[2]));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = "Old"+","+msgs[0]+"\n";
                        out.writeBytes(msgToSend);
                        out.flush();
                        InputStream inFromServer = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                        String msgFromClient = in.readLine();
                        if (msgFromClient.equals(ACK)) {
                        socket.close();
                        }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(msgs.length> 2 && msgs[2].equals("HH")){

                try {
                    Log.i("HKey",msgs[0]+"Key"+lowestHashPort+"--Lowest hashport");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lowestHashPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1]+","+"true"+"\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            //Query
            else if(msgs[1].equals("QH")){
                try{
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lowestHashPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1]+"\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    socket.close();
                    return msgFromClient;

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(msgs[1].equals("QS")){
                try{
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1]+"\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    socket.close();
                    return msgFromClient;

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(msgs[1].equals("SQ")){
                try {
                    Log.i("Succ port client",myPortNumber+"---"+msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    socket.close();
                    return msgFromClient;

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else if(msgs[1].equals("FFD")){
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(lowestHashPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }
            else if(msgs[1].equals("SFD")){

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }
                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }
            else if(msgs[1].equals("PFD")){
                try{
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succPort));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }

                }catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            else {
                try {
                    Log.i("Passing to succ",succPort+"--Succport --"+myPortNumber+"--Myport");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(succPort));
                OutputStream inputToServer = socket.getOutputStream();
                DataOutputStream out = new DataOutputStream(inputToServer);
                String msgToSend = msgs[0] + "," + msgs[1]+"\n";
                out.writeBytes(msgToSend);
                out.flush();
                InputStream inFromServer = socket.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                String msgFromClient = in.readLine();
                if (msgFromClient.equals(ACK)) {
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            }
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
}
