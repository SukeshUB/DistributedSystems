package edu.buffalo.cse.cse486586.groupmessenger2;


import android.util.Log;

import java.util.PriorityQueue;

/**
 * Created by sukes on 3/16/2017.
 */
public class CustomPriorityQueue  {
    static final String TAG = CustomPriorityQueue.class.getSimpleName();

    MessageObject[] messageObjects = new MessageObject[100000];
    static int size=0;

    public void insert(MessageObject messageObject){
        Log.i(TAG,"CPQ -"+messageObject.message);
        messageObjects[size++]=messageObject;

        if(size>=2){
            heapifyUp(size-1);
        }
    }

    public void heapifyUp(int i) {
        Log.i(TAG,"CPQ -Heapify up");

        while(i>0){

            if(i%2==0){
                MessageObject mobj1 = messageObjects[(i-2)/2];
                MessageObject mobj2 = messageObjects[i];
                if(mobj1.suggestedSequence>mobj2.suggestedSequence){
                    swap((i-2)/2,i);
                    i=(i-2)/2;
                }else if(mobj1.suggestedSequence == mobj2.suggestedSequence){
                    if(mobj1.canbeDelivered && !mobj2.canbeDelivered){
                        if(!mobj2.canbeDelivered){
                            swap((i-2)/2,i);
                            i=(i-2)/2;
                        }
                    }else if(!mobj1.canbeDelivered && !mobj2.canbeDelivered){
                        if(Integer.parseInt(mobj2.sequenceSuggestedProcess) < Integer.parseInt(mobj1.sequenceSuggestedProcess)){
                            swap((i-2)/2,i);
                            i=(i-2)/2;
                        }
                    }
                }

            }else{
                MessageObject mobj1 = messageObjects[(i-1)/2];
                MessageObject mobj2 = messageObjects[i];
                if(mobj1.suggestedSequence>mobj2.suggestedSequence){
                    swap((i-1)/2,i);
                    i=(i-1)/2;
                }else if(mobj1.suggestedSequence == mobj2.suggestedSequence){
                    if(mobj1.canbeDelivered && !mobj2.canbeDelivered){
                        if(!mobj2.canbeDelivered){
                            swap((i-1)/2,i);
                            i=(i-1)/2;
                        }
                    }else if(!mobj1.canbeDelivered && !mobj2.canbeDelivered){
                        if(Integer.parseInt(mobj2.sequenceSuggestedProcess) < Integer.parseInt(mobj1.sequenceSuggestedProcess)){
                            swap((i-1)/2,i);
                            i=(i-1)/2;
                        }
                    }
                }
            }


        }
    }

    public void swap(int index1,int index2){

        MessageObject temp = messageObjects[index1];
        messageObjects[index1]=messageObjects[index2];
        messageObjects[index2]=temp;

    }

    public MessageObject extractMin(){
        Log.i(TAG,"CPQ - Extract Min"+messageObjects[0].message);

        MessageObject res = messageObjects[0];
        messageObjects[0]=messageObjects[size-1];
        size--;
        heapifyDown(0,size-1);
        return res;

    }

    public void heapifyDown(int index,int pos){
        Log.i(TAG,"CPQ - Heapify down");

        while(index<pos){

            if(pos==(2*index+1)){
                if(messageObjects[pos].suggestedSequence<messageObjects[index].suggestedSequence) {
                    swap(pos, index);
                    index=pos;
                }else if(messageObjects[pos].suggestedSequence==messageObjects[index].suggestedSequence){
                    if(messageObjects[pos].canbeDelivered && !messageObjects[index].canbeDelivered){
                        if(!messageObjects[pos].canbeDelivered){
                            swap(pos, index);
                            index=pos;
                        }
                    }else if(!messageObjects[pos].canbeDelivered && !messageObjects[index].canbeDelivered){
                        if(Integer.parseInt(messageObjects[pos].sequenceSuggestedProcess) < Integer.parseInt(messageObjects[index].sequenceSuggestedProcess)){
                            swap(pos, index);
                            index=pos;
                        }
                    }
                }
            }else if(pos>=(2*index)+2){
                if(messageObjects[(2*index)+1].suggestedSequence<messageObjects[(2*index)+2].suggestedSequence){
                    if(messageObjects[(2*index)+1].suggestedSequence<messageObjects[index].suggestedSequence) {
                        swap((2 * index) + 1, index);
                        index = (2 * index) + 1;
                    }else if(messageObjects[(2*index)+1].suggestedSequence==messageObjects[index].suggestedSequence){
                          if(messageObjects[(2*index)+1].canbeDelivered && !messageObjects[index].canbeDelivered){
                              if(!messageObjects[(2*index)+1].canbeDelivered){
                                  swap((2 * index) + 1, index);
                                  index = (2 * index) + 1;
                              }
                          }else if(!messageObjects[(2*index)+1].canbeDelivered && !messageObjects[index].canbeDelivered){
                              if(Integer.parseInt(messageObjects[(2*index)+1].sequenceSuggestedProcess) < Integer.parseInt(messageObjects[index].sequenceSuggestedProcess)){
                                  swap(index, (2*index)+1);
                                  index=(2*index)+1;
                              }

                          }
                    }
                }else{
                    if(messageObjects[(2*index)+2].suggestedSequence<messageObjects[index].suggestedSequence) {
                        swap((2 * index) + 2, index);
                        index = (2 * index) + 2;
                    }else if(messageObjects[(2*index)+2].suggestedSequence==messageObjects[index].suggestedSequence){
                        if(messageObjects[(2*index)+2].canbeDelivered && !messageObjects[index].canbeDelivered){
                            if(!messageObjects[(2*index)+2].canbeDelivered){
                                swap((2 * index) + 2, index);
                                index = (2 * index) + 2;
                            }
                        }else if(!messageObjects[(2*index)+2].canbeDelivered && !messageObjects[index].canbeDelivered){
                            if(Integer.parseInt(messageObjects[(2*index)+2].sequenceSuggestedProcess) < Integer.parseInt(messageObjects[index].sequenceSuggestedProcess)){
                                swap(index, (2*index)+2);
                                index=(2*index)+2;
                            }

                        }
                    }
                }
            }
            else
                return;

        }

    }

    public boolean isEmpty(){
        return size==0;
    }

    public void changeSequenceValue(String messageId,String processId,String agreedSequence,String agreedSeqProcess){
        Log.i(TAG,"CPQ - Change Sequence");
        int i=0;
        for(;i<messageObjects.length;i++){

            if(messageObjects[i].messageId.equals(messageId)
                    && messageObjects[i].messageSentProcess.equals(processId))
                break;

        }

        if(Integer.parseInt(agreedSequence) > messageObjects[i].suggestedSequence){
            messageObjects[i].suggestedSequence=Integer.parseInt(agreedSequence);
            messageObjects[i].sequenceSuggestedProcess=agreedSeqProcess;
            messageObjects[i].canbeDelivered=true;
            heapifyDown(i,size-1);
        }else if(Integer.parseInt(agreedSequence) <= messageObjects[i].suggestedSequence){
            messageObjects[i].suggestedSequence=Integer.parseInt(agreedSequence);
            messageObjects[i].sequenceSuggestedProcess=agreedSeqProcess;
            messageObjects[i].canbeDelivered=true;
            heapifyUp(i);
        }

    }

    public MessageObject peek(){
        if(!isEmpty())
            return messageObjects[0];
        return null;
    }

}
