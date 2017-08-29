package edu.buffalo.cse.cse486586.simpledynamo;

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
import java.nio.Buffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String TEST_FIELD = "test";
    private static final String REDIRECT_FIELD = "redirect";
    static final String[] portArr = new String[]{"11108", "11112", "11116", "11120", "11124"};
    static String myPortNumber;
    static final int SERVER_PORT = 10000;
    static final String ACK = "ACKNOWLEDGED";
    static Map<String, String> portHashmap;
    static List<String> porthashes;
    static String myHash;
    static String mysuccport1;
    static String mysuccport2;
    static String mypredport1;
    static String mypredport2;
    static String predhash1;
    static String predhash2;
    static String minhash;
    static String maxhash;
    static String succhash;
    static List<String> failednodesvals = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        //findHashes();

        String filename = selection;
        if (filename.equals("*")) {
            String[] files = getContext().fileList();
            for (String file : files) {
                getContext().deleteFile(file);
            }

            for (int i = 0; i < porthashes.size(); i++) {
                if (porthashes.get(i).equals(myHash)) continue;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "SD", portHashmap.get(porthashes.get(i)));
            }

        } else if (filename.equals("*R")) {
            String[] files = getContext().fileList();
            for (String file : files) {
                getContext().deleteFile(file);
            }
        } else if (filename.equals("@")) {
            String[] files = getContext().fileList();
            for (String file : files) {
                getContext().deleteFile(file);
            }
        } else if (filename.contains(",")) {
            String[] names = filename.split(",");
            getContext().deleteFile(names[0]);
        } else {

            String keyHash = "";
            try {
                keyHash = genHash(filename);
            } catch (NoSuchAlgorithmException n) {
                Log.i("Insert Key Hash", "Generate key hash exception");
            }

            for (int i = 0; i < porthashes.size(); i++) {

                if (keyHash.compareTo(porthashes.get(i)) < 0) {

                    if (porthashes.get(i).equals(myHash)) {

                        getContext().deleteFile(filename);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND1", mysuccport1);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND2", mysuccport2);
                        break;

                    } else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND", portHashmap.get(porthashes.get(i)));
                        break;
                    }
                }
            }

            if (keyHash.compareTo(porthashes.get(porthashes.size() - 1)) > 0) {

                if (porthashes.get(0).equals(myHash)) {

                    getContext().deleteFile(filename);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND1", mysuccport1);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND2", mysuccport2);

                } else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "ND", portHashmap.get(porthashes.get(0)));
                }
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

        //findHashes();

        String filename = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);
        Log.i("In insert", values.size() + "--" + filename);
        String keyHash = "";
        try {
            keyHash = genHash(filename);
            Log.i("Keyhash is", keyHash);
        } catch (NoSuchAlgorithmException n) {
            Log.i("Insert Key Hash", "Generate key hash exception");
        }
        if (values.containsKey(TEST_FIELD)) {
            Log.i("Own Insert", "Own Insert");
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed" + e);
            }

/*            if (values.containsKey(REDIRECT_FIELD)) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", mysuccport1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", mysuccport2);
            }*/
        } else {
            for (int i = 0; i < porthashes.size(); i++) {
                //Log.i("InsertLoop", i + "");
                if (keyHash.compareTo(porthashes.get(i)) < 0) {
                    //Log.i("Matchinghash", "" + porthashes.get(i));
                    if (porthashes.get(i).equals(myHash)) {
                        //Internal Storage
                        //Log.i("IS", "Internal Storage" + "--" + myHash);
                        Log.i("IOS", myHash);
                        FileOutputStream outputStream;
                        try {
                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            outputStream.write(value.getBytes());
                            outputStream.close();
                        } catch (Exception e) {
                            Log.e(TAG, "File write failed" + e);
                        }
                        Log.i("Ports", mysuccport1 + "--" + mysuccport2);
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "FS1", mysuccport1, mysuccport2);
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", mysuccport1);
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", mysuccport2);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IRLM", mysuccport1, mysuccport2);
                        Log.i("FSEND", "FEND");
                        break;
                    } else {
                        //Log.i("OS", "Outside Storage");
                        //Log.i("OS", portHashmap.get(porthashes.get(i)));
                        Log.i("IRD", porthashes.get(i));
                        String cosuccport1 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 1));
                        String cosuccport2 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(1)) : i == porthashes.size() - 2 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 2));
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", portHashmap.get(porthashes.get(i)));
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", cosuccport1);
                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", cosuccport2);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IRLO", portHashmap.get(porthashes.get(i)), cosuccport1, cosuccport2);
                        Log.i("IREND", "IEND");
                        break;
                    }
                }
            }

            if (keyHash.compareTo(porthashes.get(porthashes.size() - 1)) > 0) {
                //Log.i("EdgeCaseInsert", keyHash);
                Log.i("IRDEC", porthashes.get(0));
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", portHashmap.get(porthashes.get(0)));
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", portHashmap.get(porthashes.get(1)));
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IR2", portHashmap.get(porthashes.get(2)));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, value, "IRLO", portHashmap.get(porthashes.get(0)), portHashmap.get(porthashes.get(1)), portHashmap.get(porthashes.get(2)));
                Log.i("IRDECEND", "IDECEND");
            }
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        Log.i("OnCreate", "Create");
        Log.i("Recovery Start", "RST");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        findHashes();

        Map<String, String> datamap = new HashMap<String, String>();
        Log.i("Porthashessize", porthashes.size() + "");
      /*  for (int i = 0; i < porthashes.size(); i++) {
            if (porthashes.get(i).equals(myHash)) continue;
            if (porthashes.get(i).equals(predhash1) || porthashes.get(i).equals(predhash2)) {
                Log.i("Call id", i + "--" + portHashmap.get(porthashes.get(i)));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "OCTP", portHashmap.get(porthashes.get(i)));
            } else if (porthashes.get(i).equals(succhash)) {
                Log.i("Call id", i + "--" + portHashmap.get(porthashes.get(i)));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "OCTS", portHashmap.get(porthashes.get(i)), myHash, predhash1);
            }
            Log.i("I value", i + "");
        }*/

       /* for (int i = 0; i < porthashes.size(); i++) {
            if (porthashes.get(i).equals(myHash)) continue;
            Log.i("Restore id on", i + "--" + portHashmap.get(porthashes.get(i)));*/
        Log.i(TAG,"Invocation of OCRL");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "OCRL");
   /*         Log.i("I value", i + "");
        }*/
        Log.i("EOC", "End of On Create");
        return false;
    }

    public void findHashes() {

        porthashes = new ArrayList<String>();
        /*porthashes.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
        porthashes.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");
        porthashes.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");
        porthashes.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");
        porthashes.add("c25ddd596aa7c81fa12378fa725f706d54325d12");*/

        portHashmap = new HashMap<String, String>();
      /*  portHashmap.put(porthashes.get(0),"11108");
        portHashmap.put(porthashes.get(1),"11112");
        portHashmap.put(porthashes.get(2),"11116");
        portHashmap.put(porthashes.get(3),"11120");
        portHashmap.put(porthashes.get(4),"11124");*/

        Log.i("FindHashes size", porthashes.size() + "");
        for (int i = 0; i < portArr.length; i++) {

            String port = portArr[i];
            try {
                int value = Integer.parseInt(port);
                value = value / 2;
                String portHash = genHash(String.valueOf(value));
                porthashes.add(portHash);
                portHashmap.put(portHash, port);
                if (port.equals(myPortNumber))
                    myHash = portHash;
            } catch (NoSuchAlgorithmException a) {
                Log.i(TAG, "No Hash");
            }
        }
        Collections.sort(porthashes);
        minhash = porthashes.get(0);
        maxhash = porthashes.get(porthashes.size() - 1);

        for (int i = 0; i < porthashes.size(); i++) {

            if (porthashes.get(i).equals(myHash)) {

                mysuccport1 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 1));
                mysuccport2 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(1)) : i == porthashes.size() - 2 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 2));
                mypredport1 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 1));
                mypredport2 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 2)) : i == 1 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 2));
                Log.i("Succport1", mysuccport1);
                Log.i("Succport2", mysuccport2);
                Log.i("Predport1", mypredport1);
                Log.i("Predport2", mypredport2);
                predhash1 = i == 0 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 1);
                predhash2 = i == 0 ? porthashes.get(porthashes.size() - 2) : i == 1 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 2);
                succhash = i == porthashes.size() - 1 ? porthashes.get(0) : porthashes.get(i + 1);
                break;
            }
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        //findHashes();
        String filename = selection;
        FileInputStream inputStream;
        String result = "";
        if (filename.equals("*")) {
            //Log.i("InSQ", "Original");
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            MatrixCursor mc = new MatrixCursor(columns);

            for (int i = 0; i < porthashes.size(); i++) {
                if (porthashes.get(i).equals(myHash)) continue;
                try {
                    String querystarres = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portHashmap.get(porthashes.get(i)), "SQ").get();
                    if (querystarres != null && querystarres.length() > 0) {
                        String[] vals = querystarres.split(",");
                        for (String value : vals) {
                            String[] keyval = value.split("#");
                            mc.addRow(keyval);
                        }
                    }
                } catch (InterruptedException e) {
                    Log.i(TAG, e.getMessage());
                } catch (ExecutionException a) {
                    Log.i(TAG, a.getMessage());
                }
            }
            //Log.i("OwnStar", "In Own star");
            String[] files = getContext().fileList();
            for (String file : files) {
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
                result = "";
            }
            return mc;

        } else if (filename.equals("@")) {
            String[] files = getContext().fileList();
            String[] columns = new String[2];
            columns[0] = KEY_FIELD;
            columns[1] = VALUE_FIELD;
            MatrixCursor mc = new MatrixCursor(columns);
            for (String file : files) {

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
                result = "";
            }
            Log.v("query@", selection);
            return mc;
        } else if (filename.contains(",")) {
            String[] names = filename.split(",");
            filename = names[0];
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
            //Log.v("query", selection);
            return mc;
        } else {
            String hashKey = "";
            try {
                hashKey = genHash(filename);
            } catch (NoSuchAlgorithmException n) {
                Log.i("Selection hash", "Selection");
            }

            for (int i = 0; i < porthashes.size(); i++) {
                try {
                    if (hashKey.compareTo(porthashes.get(i)) < 0) {
                        Log.i("Last replica on", porthashes.get(i) + "--" + portHashmap.get(porthashes.get(i)));
                        String value = "";
                        //Last replica is in i+2
                      /*  if (i == porthashes.size() - 1) {
                            Log.i("Last replica is", porthashes.get(i) + "--" + portHashmap.get(porthashes.get(1)));
                            value = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QR", portHashmap.get(porthashes.get(i))).get();
                        } else if (i == porthashes.size() - 2) {
                            Log.i("Last replica is", porthashes.get(i) + "--" + portHashmap.get(porthashes.get(0)));
                            value = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QR", portHashmap.get(porthashes.get(i))).get();
                        } else {*/

                        //Log.i("Last replica is", porthashes.get(i) + "--" + portHashmap.get(porthashes.get(i + 2)));
                        String cosuccport1 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 1));
                        String cosuccport2 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(1)) : i == porthashes.size() - 2 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 2));

                        value = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QR", portHashmap.get(porthashes.get(i)), cosuccport1, cosuccport2).get();

                        String[] columns = new String[2];
                        columns[0] = KEY_FIELD;
                        columns[1] = VALUE_FIELD;
                        String[] values = new String[2];
                        values[0] = filename;
                        values[1] = value;
                        MatrixCursor mc = new MatrixCursor(columns);
                        mc.addRow(values);
                        Log.i("queryanswer", filename + "--" + value);
                        return mc;
                    }
                } catch (InterruptedException e) {
                    Log.i(TAG, "Interrupted Exception");
                } catch (ExecutionException a) {
                    Log.i(TAG, "Execution Exception");
                }
            }

            if (hashKey.compareTo(porthashes.get(porthashes.size() - 1)) > 0) {
                try {
                    String value = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, filename, "QR", portHashmap.get(porthashes.get(0)), portHashmap.get(porthashes.get(1)), portHashmap.get(porthashes.get(2))).get();
                    String[] columns = new String[2];
                    columns[0] = KEY_FIELD;
                    columns[1] = VALUE_FIELD;
                    String[] values = new String[2];
                    values[0] = filename;
                    values[1] = value;
                    MatrixCursor mc = new MatrixCursor(columns);
                    mc.addRow(values);
                    //Log.v("query", selection);
                    return mc;
                } catch (InterruptedException e) {
                    Log.i(TAG, "Interrupted Exception");
                } catch (ExecutionException a) {
                    Log.i(TAG, "Execution Exception");
                }
            }

        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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
            while (true) {
                try {
                    Socket server = serverSocket.accept();
                    InputStream inputFromClient = server.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputFromClient));
                    String input = "";

                    if ((input = in.readLine()) != null) {
                        Log.i("InServerChecks", "Inserver");
                        String[] msgs = input.split(",");
                        if (msgs.length > 2 && msgs[2].equals("IR")) {

                            Log.i("original-OutsideStorage", "Original-OutsideStorage-Server" + "--" + msgs[0]);
                            //ContentResolver cr = getContext().getContentResolver();
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgs[0]);
                            cv.put(VALUE_FIELD, msgs[1]);
                            cv.put(TEST_FIELD, "RD");
                            cv.put(REDIRECT_FIELD, "true");
                            insert(uri, cv);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            //publishProgress(msgs[0], msgs[1], msgs[2]);
                            Log.i("Ackendserver", "Ackendserver");
                        } else if (msgs.length > 2 && msgs[2].equals("IR1")) {

                            Log.i("R1-OutsideStorage", "R1-OutsideStorage-Server" + "---" + msgs[0]);
                            //ContentResolver cr = getContext().getContentResolver();
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgs[0]);
                            cv.put(VALUE_FIELD, msgs[1]);
                            cv.put(TEST_FIELD, "RD");
                            insert(uri, cv);

                            Log.i("APPIR1", "After publish progress");
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            publishProgress(msgs[0], msgs[1], msgs[2], msgs[3]);
                            Log.i("AckendserverIR1", "Ackendserver");
                        } else if (msgs.length > 2 && msgs[2].equals("IR2")) {

                            Log.i("Insertion", "Insertion of" + "---" + msgs[0] + "--on" + myPortNumber);
                            //ContentResolver cr = getContext().getContentResolver();
                            /*Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgs[0]);
                            cv.put(VALUE_FIELD, msgs[1]);
                            cv.put(TEST_FIELD, "RD");*/
                            FileOutputStream outputStream;
                            try {
                                outputStream = getContext().openFileOutput(msgs[0], Context.MODE_PRIVATE);
                                outputStream.write(msgs[1].getBytes());
                                outputStream.close();
                            } catch (Exception e) {
                                Log.e(TAG, "File write failed" + e);
                            }
                            //insert(uri, cv);
                            Log.i("After insert", "insertIR2");
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            Log.i("AckendserverIR2", "Ackendserver");
                        } else if (msgs.length > 2 && msgs[2].equals("FS1")) {

                            Log.i("R1-OwnClient-Server", "R1-OwnClient-Server" + "---" + msgs[0]);
                            //ContentResolver cr = getContext().getContentResolver();
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgs[0]);
                            cv.put(VALUE_FIELD, msgs[1]);
                            cv.put(TEST_FIELD, "RD");
                            insert(uri, cv);

                            Log.i("APPFS1", "After publish progress");
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            publishProgress(msgs[0], msgs[1], msgs[2], msgs[3]);
                            Log.i("AckendserverFS1", "ackend server");
                        } else if (msgs.length > 2 && msgs[2].equals("FS2")) {
                            Log.i("R2-OwnClient-Server", "R2-OwnClient-Server" + "--" + msgs[0]);
                            //ContentResolver cr = getContext().getContentResolver();
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgs[0]);
                            cv.put(VALUE_FIELD, msgs[1]);
                            cv.put(TEST_FIELD, "RD");
                            insert(uri, cv);
                            Log.i("InsertFS2", "FS2Server");
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            Log.i("ackend", "After ackend");
                        } else if (msgs.length > 1 && msgs[1].equals("QR")) {

                            Log.i("In QR", "QR");
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Cursor rc = query(uri, null, msgs[0] + ",R", null, null);
                            int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                            rc.moveToFirst();
                            String returnValue = rc.getString(valueIndex);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = returnValue + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        } else if (msgs[0].equals("SQ")) {
                            //Log.i("In SQServer", msgs[0]);
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Cursor rc = query(uri, null, "@", null, null);
                            StringBuffer sb = new StringBuffer();
                            //Log.i("Rc count", "SQ" + rc.getCount());
                            String resultStar = "";
                            if (rc.getCount() > 0) {
                                while (rc.moveToNext()) {
                                    //Log.i("In Cursor iterator", rc.toString());
                                    int keyIndex = rc.getColumnIndex(KEY_FIELD);
                                    int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                                    String returnKey = rc.getString(keyIndex);
                                    String returnValue = rc.getString(valueIndex);
                                    sb.append(returnKey).append("#").append(returnValue).append(",");
                                }
                                sb.deleteCharAt(sb.length() - 1);
                                resultStar = sb.toString();
                            }
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = resultStar + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        } else if (msgs.length > 1 && msgs[1].equals("ND")) {
                            //isDeleteDirected =true;
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            delete(uri, msgs[0] + ",R", null);

                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                            publishProgress(msgs[0], msgs[1]);

                        } else if (msgs.length > 1 && (msgs[1].equals("ND1") || msgs[1].equals("ND2"))) {
                            //isDeleteDirected =true;
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            delete(uri, msgs[0] + ",R", null);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();

                        } else if (msgs.length > 1 && msgs[1].equals("SD")) {
                            //isDeleteDirected =true;
                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            delete(uri, msgs[0] + "R", null);
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = ACK + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();

                        } else if (msgs[0].equals("OCTP")) {

                            porthashes = new ArrayList<String>();
                            portHashmap = new HashMap<String, String>();

                            Log.i("FindHashes size", porthashes.size() + "");

                            for (int i = 0; i < portArr.length; i++) {

                                String port = portArr[i];
                                try {
                                    int value = Integer.parseInt(port);
                                    value = value / 2;
                                    String portHash = genHash(String.valueOf(value));
                                    porthashes.add(portHash);
                                    portHashmap.put(portHash, port);
                                    if (port.equals(myPortNumber))
                                        myHash = portHash;
                                } catch (NoSuchAlgorithmException a) {
                                    Log.i(TAG, "No Hash");
                                }
                            }
                            Collections.sort(porthashes);
                            minhash = porthashes.get(0);
                            maxhash = porthashes.get(porthashes.size() - 1);

                            for (int i = 0; i < porthashes.size(); i++) {

                                if (porthashes.get(i).equals(myHash)) {

                                    mysuccport1 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 1));
                                    mysuccport2 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(1)) : i == porthashes.size() - 2 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 2));
                                    mypredport1 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 1));
                                    mypredport2 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 2)) : i == 1 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 2));
                                    Log.i("Succport1", mysuccport1);
                                    Log.i("Succport2", mysuccport2);
                                    Log.i("Predport1", mypredport1);
                                    Log.i("Predport2", mypredport2);
                                    predhash1 = i == 0 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 1);
                                    predhash2 = i == 0 ? porthashes.get(porthashes.size() - 2) : i == 1 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 2);
                                    succhash = i == porthashes.size() - 1 ? porthashes.get(0) : porthashes.get(i + 1);
                                    break;
                                }
                            }

                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Cursor rc = query(uri, null, "@", null, null);
                            String resultVal = "";
                            StringBuffer sb = new StringBuffer();
                            Log.i("Rc count", "OCTP-Server" + rc.getCount());
                            if (rc.getCount() > 0) {
                                while (rc.moveToNext()) {
                                    Log.i("RC Position", rc.getPosition() + "");
                                    //Log.i("In Cursor iterator", rc.toString());
                                    int keyIndex = rc.getColumnIndex(KEY_FIELD);
                                    int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                                    String returnKey = rc.getString(keyIndex);
                                    String returnValue = rc.getString(valueIndex);
                                    Log.i("KV", returnKey + "--" + returnValue);
                                    String keycheckhash = "";
                                    try {
                                        keycheckhash = genHash(returnKey);
                                    } catch (NoSuchAlgorithmException n) {
                                    }
                                    if (keycheckhash.compareTo(predhash1) > 0 && keycheckhash.compareTo(myHash) < 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    } else if (myHash.equals(minhash) && keycheckhash.compareTo(predhash1) > 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    } else if (myHash.equals(minhash) && keycheckhash.compareTo(myHash) < 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    }
                                }
                                if (sb.length() > 0)
                                    sb.deleteCharAt(sb.length() - 1);
                                resultVal = sb.toString();
                            }
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = resultVal + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        } else if (msgs[0].equals("OCTS")) {

                            porthashes = new ArrayList<String>();
                            portHashmap = new HashMap<String, String>();

                            Log.i("FindHashes size", porthashes.size() + "");

                            for (int i = 0; i < portArr.length; i++) {

                                String port = portArr[i];
                                try {
                                    int value = Integer.parseInt(port);
                                    value = value / 2;
                                    String portHash = genHash(String.valueOf(value));
                                    porthashes.add(portHash);
                                    portHashmap.put(portHash, port);
                                    if (port.equals(myPortNumber))
                                        myHash = portHash;
                                } catch (NoSuchAlgorithmException a) {
                                    Log.i(TAG, "No Hash");
                                }
                            }
                            Collections.sort(porthashes);
                            minhash = porthashes.get(0);
                            maxhash = porthashes.get(porthashes.size() - 1);

                            for (int i = 0; i < porthashes.size(); i++) {

                                if (porthashes.get(i).equals(myHash)) {

                                    mysuccport1 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 1));
                                    mysuccport2 = i == porthashes.size() - 1 ? portHashmap.get(porthashes.get(1)) : i == porthashes.size() - 2 ? portHashmap.get(porthashes.get(0)) : portHashmap.get(porthashes.get(i + 2));
                                    mypredport1 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 1));
                                    mypredport2 = i == 0 ? portHashmap.get(porthashes.get(porthashes.size() - 2)) : i == 1 ? portHashmap.get(porthashes.get(porthashes.size() - 1)) : portHashmap.get(porthashes.get(i - 2));
                                    Log.i("Succport1", mysuccport1);
                                    Log.i("Succport2", mysuccport2);
                                    Log.i("Predport1", mypredport1);
                                    Log.i("Predport2", mypredport2);
                                    predhash1 = i == 0 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 1);
                                    predhash2 = i == 0 ? porthashes.get(porthashes.size() - 2) : i == 1 ? porthashes.get(porthashes.size() - 1) : porthashes.get(i - 2);
                                    succhash = i == porthashes.size() - 1 ? porthashes.get(0) : porthashes.get(i + 1);
                                    break;
                                }
                            }

                            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Cursor rc = query(uri, null, "@", null, null);
                            String resultVal = "";
                            StringBuffer sb = new StringBuffer();
                            Log.i("Rc count", "OCTS-Server" + rc.getCount());
                            if (rc.getCount() > 0) {
                                while (rc.moveToNext()) {
                                    Log.i("RC Position", rc.getPosition() + "");
                                    //Log.i("In Cursor iterator", rc.toString());
                                    int keyIndex = rc.getColumnIndex(KEY_FIELD);
                                    int valueIndex = rc.getColumnIndex(VALUE_FIELD);
                                    String returnKey = rc.getString(keyIndex);
                                    String returnValue = rc.getString(valueIndex);
                                    Log.i("KV", returnKey + "--" + returnValue);
                                    String keycheckhash = "";
                                    try {
                                        keycheckhash = genHash(returnKey);
                                    } catch (NoSuchAlgorithmException n) {
                                    }
                                    if (keycheckhash.compareTo(msgs[3]) > 0 && keycheckhash.compareTo(msgs[2]) < 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    } else if (msgs[2].equals(minhash) && keycheckhash.compareTo(msgs[3]) > 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    } else if (msgs[2].equals(minhash) && keycheckhash.compareTo(msgs[2]) < 0) {
                                        sb.append(returnKey).append("#").append(returnValue).append(",");
                                    }
                                }
                                if (sb.length() > 0)
                                    sb.deleteCharAt(sb.length() - 1);
                                resultVal = sb.toString();
                            }
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = resultVal + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }else if (msgs[0].equals("OCRL")){

                            StringBuffer lsb = new StringBuffer();
                            if(failednodesvals.size()>0){
                                for(String iv : failednodesvals){
                                    lsb.append(iv).append(",");
                                }
                                lsb.deleteCharAt(lsb.length()-1);
                                failednodesvals.clear();
                            }
                            OutputStream outToClient = server.getOutputStream();
                            DataOutputStream out = new DataOutputStream(outToClient);
                            String dataToSend = lsb.toString() + "\n";
                            out.writeBytes(dataToSend);
                            out.flush();
                        }
                    }
                    server.close();
                } catch (IOException i) {
                    Log.i("FailedNode in Server", "FNS");
                    Log.i(TAG, "IO Exception");
                }

            }

        }

        protected void onProgressUpdate(String... strings) {
            //Log.i("InOnprogess",strings[2]);
            if (strings.length > 2 && strings[2].equals("IR")) {
                Log.i("BeforeOP", "IR");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1], "IR1", mysuccport1, mysuccport2);
                Log.i("AfterOP", "IR");
            } else if (strings.length > 2 && strings[2].equals("IR1")) {
                Log.i("BeforeOP", "IR1");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1], "IR2", strings[3]);
                Log.i("AfterOP", "IR1");
            } else if (strings.length > 2 && strings[2].equals("FS1")) {
                Log.i("BeforeOP", "FS1");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1], "FS2", strings[3]);
                Log.i("AfterOP", "FS1");
            } else if (strings[1].equals("ND")) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], "ND1", mysuccport1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], "ND2", mysuccport2);
            }
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            if (msgs.length > 2 && msgs[2].equals("IR")) {

                try {
                    Log.i("Insert Redirection", "Original -- " + msgs[3] + "--" + msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + msgs[2] + "\n";
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
                } catch (NullPointerException n) {
                    //Send the msg to servers next two ports
                    Log.i("Failed Port is", msgs[3] + "--" + msgs[0]);
                    String porthash = "";
                    try {
                        int value = Integer.parseInt(msgs[3]) / 2;
                        porthash = genHash(String.valueOf(value));
                        Log.i("Porthash", porthash);
                    } catch (NoSuchAlgorithmException a) {
                        //Log.i("HashonPort",msgs[3]);
                    }

                    String fp1 = "";
                    String fp2 = "";
                    for (int i = 0; i < porthashes.size(); i++) {

                        if (porthashes.get(i).equals(porthash)) {

                            if (i == porthashes.size() - 1) {
                                fp1 = porthashes.get(0);
                                fp2 = porthashes.get(1);
                            } else if (i == porthashes.size() - 2) {
                                fp1 = porthashes.get(i + 1);
                                fp2 = porthashes.get(0);
                            } else {
                                fp1 = porthashes.get(i + 1);
                                fp2 = porthashes.get(i + 2);
                            }
                            Log.i("Fp1", fp1 + "" + portHashmap.get(fp1));
                            Log.i("fp2", fp2 + "" + portHashmap.get(fp2));
                            break;
                        }
                    }
                    try {
                        Log.i("Exception IR1", "Replica 1-- " + portHashmap.get(fp1) + "--" + msgs[0]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(fp1)));

                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
                        out.writeBytes(msgToSend);
                        out.flush();
                        InputStream inFromServer = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                        String msgFromClient = in.readLine();
                        if (msgFromClient.equals(ACK)) {
                            socket.close();
                        }
                        //Log.i("Failed Node", msgs[3]);
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    }

                    try {
                        Log.i("Exception IR1", "Replica 2-- " + portHashmap.get(fp2) + "--" + msgs[0]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(fp2)));

                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
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
            } else if (msgs.length > 2 && msgs[2].equals("IR1")) {

                try {
                    Log.i("Insert Redirection", "Replica 1 -- " + msgs[3] + "---" + msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    Log.i("Socketcreated", msgs[3]);
                    OutputStream inputToServer = socket.getOutputStream();
                    Log.i("AOS", "AOS");
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    Log.i("DOS", "DOS");
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR1" + "," + msgs[4] + "\n";
                    out.writeBytes(msgToSend);
                    Log.i("AWB", "AWB");
                    out.flush();
                    Log.i("In Bw Ios", "Between");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }
                    Log.i("After ack", "ack");
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    try {
                        Log.i("Exception IR2", "Replica 2-- " + msgs[3] + "---" + msgs[0]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[4]));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
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
                    //Log.i("Failed Node", msgs[3]);
                }
            } else if (msgs.length > 2 && msgs[2].equals("IR2")) {
                try {
                    Log.i("Insert Redirection", "Replica 2-- " + msgs[3] + "---" + msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
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
                } catch (NullPointerException n) {
                    Log.i("Failed Node", msgs[3]);
                }
            } else if (msgs.length > 2 && msgs[2].equals("IRLM")) {

                String[] ports = new String[]{msgs[3], msgs[4]};
                int count = 1;
                for (String port : ports) {
                    try {
                        Log.i("Insert Redirection", "Just Replicas -- " + count + "---" + port + "---" + msgs[0]);
                        count++;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
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
                    } catch (NullPointerException n) {
                        failednodesvals.add(msgs[0] + "#" + msgs[1]);
                        Log.i("Failed Node", port);
                    }
                }
            } else if (msgs.length > 2 && msgs[2].equals("IRLO")) {
                String[] ports = new String[]{msgs[3], msgs[4], msgs[5]};
                int count = 1;
                for (String port : ports) {
                    try {
                        Log.i("Insert Redirection", "With Own + Replicas-- " + count + "---" + port + "---" + msgs[0]);
                        count++;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "IR2" + "\n";
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
                    } catch (NullPointerException n) {
                        failednodesvals.add(msgs[0] + "#" + msgs[1]);
                        Log.i("Failed Node", port);
                    }
                }
            } else if (msgs.length > 2 && msgs[2].equals("FS1")) {
                try {
                    Log.i("R1-FromOwnClient", "To - " + msgs[3] + "--" + msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    Log.i("Socketcreated", msgs[3]);
                    OutputStream inputToServer = socket.getOutputStream();
                    Log.i("AOS", "AOS");
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    Log.i("DOS", "DOS");
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + msgs[2] + "," + msgs[4] + "\n";
                    out.writeBytes(msgToSend);
                    Log.i("AWB", "AWB");
                    out.flush();
                    Log.i("In Bw Ios", "Between");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }
                    Log.i("After ack", "ack");
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {

                    try {
                        Log.i("Exception handled", "To-- " + msgs[3] + "---" + msgs[0]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[4]));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "," + "FS2" + "\n";
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
                    //Log.i("FailedNode is", msgs[3]);
                }
            } else if (msgs.length > 2 && msgs[2].equals("FS2")) {
                try {
                    Log.i("R2-FromOwnClient", "To-- " + msgs[3] + "---" + msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    Log.i("Socketcreated", msgs[3]);
                    OutputStream inputToServer = socket.getOutputStream();
                    Log.i("AOS", "AOS");
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    Log.i("DOS", "DOS");
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + msgs[2] + "\n";
                    out.writeBytes(msgToSend);
                    Log.i("AWB", "AWB");
                    out.flush();
                    Log.i("In Bw Ios", "Between");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.equals(ACK)) {
                        socket.close();
                    }
                    Log.i("After ack", "ack");
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    Log.i("DOingNothing", "DN");
                    Log.i("Failed Node", msgs[3]);
                }
            } else if (msgs.length > 1 && msgs[1].equals("QR")) {

                String[] ports = new String[]{msgs[2], msgs[3], msgs[4]};
                Map<String, Integer> countmap = new LinkedHashMap<String, Integer>();
                for (String port : ports) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                        out.writeBytes(msgToSend);
                        out.flush();
                        InputStream inFromServer = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                        String msgFromClient = in.readLine();
                        if (msgFromClient.length() > 0) {
                            socket.close();
                            if (!countmap.containsKey(msgFromClient))
                                countmap.put(msgFromClient, 1);
                            else
                                countmap.put(msgFromClient, countmap.get(msgFromClient) + 1);
                        }
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    } catch (NullPointerException n) {
                        Log.i(TAG, "Failed node -- QR");
                    }
                }
                String firstkey = "";
                for (String key : countmap.keySet()) {
                    if (firstkey.isEmpty()) firstkey = key;
                    if (countmap.get(key) > 1)
                        return key;
                }
                return firstkey;
                /*try {
                    Log.i("QR to last replica", msgs[2]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[2]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient.length() > 0) {
                        socket.close();
                        return msgFromClient;
                    }*/ /*else if (msgFromClient.length() == 0) {
                        Log.i("Msglength=0", "msg length equal to 0");
                        String porthash = "";
                        try {
                            int value = Integer.parseInt(msgs[2]) / 2;
                            porthash = genHash(String.valueOf(value));
                            Log.i("Porthash", porthash);
                        } catch (NoSuchAlgorithmException a) {
                            //Log.i("HashonPort",msgs[3]);
                        }

                        porthashes = new ArrayList<String>();
                        portHashmap = new HashMap<String, String>();

                        Log.i("FindHashes size", porthashes.size() + "");

                        for (int i = 0; i < portArr.length; i++) {

                            String port = portArr[i];
                            try {
                                int value = Integer.parseInt(port);
                                value = value / 2;
                                String portHash = genHash(String.valueOf(value));
                                porthashes.add(portHash);
                                portHashmap.put(portHash, port);
                                if (port.equals(myPortNumber))
                                    myHash = portHash;
                            } catch (NoSuchAlgorithmException a) {
                                Log.i(TAG, "No Hash");
                            }
                        }
                        Collections.sort(porthashes);
                        String pp = "";
                        for (int i = 0; i < porthashes.size(); i++) {
                            if (porthashes.get(i).equals(porthash)) {

                                if (i == porthashes.size() - 1) {
                                    pp = porthashes.get(0);
                                } else {
                                    pp = porthashes.get(i + 1);
                                }
                                Log.i("PP", pp + "" + portHashmap.get(pp));
                                break;
                            }
                        }

                        try {
                            Log.i("QR- LBR-Concurrent", portHashmap.get(pp));
                            Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(portHashmap.get(pp)));
                            OutputStream inputToServer2 = socket2.getOutputStream();
                            DataOutputStream out2 = new DataOutputStream(inputToServer2);
                            String msgToSend2 = msgs[0] + "," + msgs[1] + "\n";
                            out2.writeBytes(msgToSend2);
                            out2.flush();
                            Log.i("Betweenios", "QR-LBR");
                            InputStream inFromServer2 = socket2.getInputStream();
                            BufferedReader in2 = new BufferedReader(new InputStreamReader(inFromServer2));
                            String msgFromClient2 = in2.readLine();
                            socket2.close();
                            Log.i("Endreturn", "QR_LBR");
                            return msgFromClient2;

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            e.printStackTrace();
                            Log.e(TAG, "ClientTask socket IOException");
                        }

                    }*/

               /* } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {*/

                 /*   String porthash = "";
                    try {
                        int value = Integer.parseInt(msgs[2]) / 2;
                        porthash = genHash(String.valueOf(value));
                        Log.i("Porthash", porthash);
                    } catch (NoSuchAlgorithmException a) {
                        //Log.i("HashonPort",msgs[3]);
                    }

                    porthashes = new ArrayList<String>();
                    portHashmap = new HashMap<String, String>();

                    Log.i("FindHashes size", porthashes.size() + "");

                    for (int i = 0; i < portArr.length; i++) {

                        String port = portArr[i];
                        try {
                            int value = Integer.parseInt(port);
                            value = value / 2;
                            String portHash = genHash(String.valueOf(value));
                            porthashes.add(portHash);
                            portHashmap.put(portHash, port);
                            if (port.equals(myPortNumber))
                                myHash = portHash;
                        } catch (NoSuchAlgorithmException a) {
                            Log.i(TAG, "No Hash");
                        }
                    }
                    Collections.sort(porthashes);

                    String pp = "";
                    for (int i = 0; i < porthashes.size(); i++) {
                        if (porthashes.get(i).equals(porthash)) {

                            if (i == porthashes.size() - 1) {
                                pp = porthashes.get(0);
                            } else {
                                pp = porthashes.get(i + 1);
                            }
                            Log.i("PP", pp + "" + portHashmap.get(pp));
                            break;
                        }
                    }

                    try {
                        Log.i("QR- LB replica", portHashmap.get(pp));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(pp)));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + msgs[1] + "\n";
                        out.writeBytes(msgToSend);
                        out.flush();
                        Log.i("Betweenios", "QR-LB_Nullpointer");
                        InputStream inFromServer = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                        String msgFromClient = in.readLine();
                        socket.close();
                        Log.i("Endreturn", "QR-LB Nullpointer");
                        return msgFromClient;

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    }*/
                //}
            } else if (msgs.length > 1 && msgs[1].equals("SQ")) {
                try {
                    //Log.i("SQ on", msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[1] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    //Log.i("BetweenIO", "betweenio");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    //Log.i("Msg is", msgFromClient);
                    socket.close();
                    return msgFromClient;

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask Null pointer on SQ");
                }
            } else if (msgs.length > 1 && msgs[1].equals("ND")) {
                try {
                    Log.i("ND on", msgs[2]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[2]));
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

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {

                    Log.i("Failed Port is", msgs[2] + "--" + msgs[0]);
                    String porthash = "";
                    try {
                        int value = Integer.parseInt(msgs[2]) / 2;
                        porthash = genHash(String.valueOf(value));
                        Log.i("Porthash", porthash);
                    } catch (NoSuchAlgorithmException a) {
                        //Log.i("HashonPort",msgs[3]);
                    }

                    String fp1 = "";
                    String fp2 = "";
                    for (int i = 0; i < porthashes.size(); i++) {

                        if (porthashes.get(i).equals(porthash)) {

                            if (i == porthashes.size() - 1) {
                                fp1 = porthashes.get(0);
                                fp2 = porthashes.get(1);
                            } else if (i == porthashes.size() - 2) {
                                fp1 = porthashes.get(i + 1);
                                fp2 = porthashes.get(0);
                            } else {
                                fp1 = porthashes.get(i + 1);
                                fp2 = porthashes.get(i + 2);
                            }
                            Log.i("Fp1", fp1 + "" + portHashmap.get(fp1));
                            Log.i("fp2", fp2 + "" + portHashmap.get(fp2));
                            break;
                        }
                    }

                    try {
                        Log.i("ND1 on", portHashmap.get(fp1));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(fp1)));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + "ND1" + "\n";
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

                    try {
                        Log.i("ND2 on", portHashmap.get(fp2));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(fp2)));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "," + "ND2" + "\n";
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
            } else if (msgs.length > 1 && (msgs[1].equals("ND1") || msgs[1].equals("ND2"))) {
                try {
                    //Log.i("ND on", msgs[0]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[2]));
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
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    Log.i(TAG, "Delete on failed port");
                }
            } else if (msgs.length > 1 && msgs[1].equals("SD")) {
                try {
                    //Log.i("SD on", msgs[2]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[2]));
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

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    Log.i(TAG, "Port failed");
                }
            } else if (msgs[0].equals("OCTP")) {
                try {
                    Log.i("OCTP on FCTS", msgs[1]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    //Log.i("BetweenIO", "betweenio");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    if (msgFromClient != null && !msgFromClient.isEmpty()) {
                        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        String[] vals1 = msgFromClient.split(",");
                        Log.i("Vals1length", vals1.length + "");
                        //String[] vals2 = two.split(",");
                        for (String value : vals1) {
                            Log.i("InFirstloop", value);
                            String[] keyval = value.split("#");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, keyval[0]);
                            cv.put(VALUE_FIELD, keyval[1]);
                            cv.put(TEST_FIELD, "RD");
                            insert(uri, cv);
                        }
                    }
                    Log.i("Msg is", msgFromClient + "");
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    Log.i("Failed Node", "OCT Starup-Node didn't join yet");
                }
            } else if (msgs[0].equals("OCTS")) {
                try {
                    Log.i("OCTS on FCTS", msgs[1]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    OutputStream inputToServer = socket.getOutputStream();
                    DataOutputStream out = new DataOutputStream(inputToServer);
                    String msgToSend = msgs[0] + "," + msgs[1] + "," + msgs[2] + "," + msgs[3] + "\n";
                    out.writeBytes(msgToSend);
                    out.flush();
                    //Log.i("BetweenIO", "betweenio");
                    InputStream inFromServer = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                    String msgFromClient = in.readLine();
                    Log.i("Msg is", msgFromClient + "");
                    if (msgFromClient != null && !msgFromClient.isEmpty()) {
                        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        String[] vals1 = msgFromClient.split(",");
                        Log.i("Vals1length", vals1.length + "");
                        //String[] vals2 = two.split(",");
                        for (String value : vals1) {
                            Log.i("InFirstloop", value);
                            String[] keyval = value.split("#");
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, keyval[0]);
                            cv.put(VALUE_FIELD, keyval[1]);
                            cv.put(TEST_FIELD, "RD");
                            insert(uri, cv);
                        }
                    }
                    socket.close();


                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (NullPointerException n) {
                    Log.i("Failed Node", "OCT Starup-Node didn't join yet");
                }
            } else if (msgs[0].equals("OCRL")) {
                for (int i = 0; i < porthashes.size(); i++) {
                    if(porthashes.get(i).equals(myHash)) continue;
                    try {
                        Log.i("OCRL on", portHashmap.get(porthashes.get(i)));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portHashmap.get(porthashes.get(i))));
                        OutputStream inputToServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(inputToServer);
                        String msgToSend = msgs[0] + "\n";
                        out.writeBytes(msgToSend);
                        out.flush();
                        InputStream inFromServer = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inFromServer));
                        String msgFromClient = in.readLine();
                        if (msgFromClient != null && !msgFromClient.isEmpty()) {
                            String[] vals1 = msgFromClient.split(",");
                            Log.i("Vals1length", vals1.length + "");
                            for (String value : vals1) {
                                String[] keyval = value.split("#");
                                FileOutputStream outputStream;
                                try {
                                    outputStream = getContext().openFileOutput(keyval[0], Context.MODE_PRIVATE);
                                    outputStream.write(keyval[1].getBytes());
                                    outputStream.close();
                                } catch (Exception e) {
                                    Log.e(TAG, "File write failed" + e);
                                }
                            }
                        }
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "ClientTask socket IOException");
                    } catch (NullPointerException n) {
                        Log.i(TAG, "Port failed");
                    }

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
