package simpledb;

import java.util.*;

public class LRUCache {
    private HashMap<PageId, Page> map;
    private LRUList lruList;
    private final int MAX_CAPACITY;

    public LRUCache(int capacity) {
        this.map = new HashMap<>();
        this.lruList = new LRUList(capacity);
        this.MAX_CAPACITY = capacity;
    }

    public HashMap<PageId, Page> getMap(){
        return map;
    }

    public Page get(PageId key) {
        Page temp = map.get(key);
        if (temp != null){
            try{
                lruList.add(lruList.remove(key));
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return temp;
    }
    
    public void put(PageId key, Page value) {
        lruList.add(key);
        map.put(key, value);
        
        //LRU policy
        if (lruList.size() > MAX_CAPACITY) {
            PageId head = lruList.getHead();
            try{
                lruList.remove(head);
            } catch (Exception e){
                e.printStackTrace();
            }
            map.remove(head);
        }
    }

}

class LRUList{

    class Node{
        PageId pageId;
        Node pre;
        Node next;

        public Node(PageId pageId){
            this.pageId = pageId;
            pre = null;
            next = null;
        }

        @Override
        public boolean equals(Object o){
            return pageId.equals(o);
        }
    }

    private Node head;
    private Node tail;
    private int size;

    public int size(){
        return size;
    }

    public LRUList(int capacity){
        this.size = 0;
        this.head = null;
        this.tail = null;
    }

    //添加节点必定是往最后添加
    public void add(PageId pageId){
        Node newNode = new Node(pageId);
        if (head == null){
            head = newNode;
            tail = head;
        } else {
            tail.next = newNode;
            newNode.pre = tail;
            tail = newNode;
        }

        this.size++;
    } 

    //移除节点有移除头部，移除中间
    public PageId remove(PageId pageId) throws Exception {
        if (head == null) return null;

        this.size--;
        if (head.equals(pageId)){
            head = head.next;
            if (head != null){
                head.pre = null;
            }
            return pageId;
        } else {
            Node headCopy = head;
            while (!headCopy.equals(pageId) && headCopy.next != null) {
                headCopy = headCopy.next;
            }

            if (headCopy.next == null) {
                if (!headCopy.equals(pageId)){
                    throw new Exception("can't reach here!");
                }

                tail = headCopy.pre;

                tail.next = null;
                headCopy.pre = null;
                
            } else{
            
                headCopy.pre.next = headCopy.next;
                headCopy.next.pre = headCopy.pre;
    
                headCopy.next = null;
                headCopy.pre = null;
            
            }
            return headCopy.pageId;
        }
    }

    public PageId getHead() {
        return head.pageId;
    }

}

