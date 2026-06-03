/*
MIT License

Copyright (c) 2019 JuanBesa

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package SimpleConcurrent;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Imported from https://github.com/JuanBesa/Concurrent-RedBlack-Tree
 */
@SuppressWarnings("unused")
public class ConcurrentRBTree<K, V> extends AbstractMap<K, V> {
			
    /** This is a special value that indicates the presence of a null value,
     *  to differentiate from the absence of a value.
     */
    static final Object SpecialNull = new Object();

    /** This is a special value that indicates that an optimistic read
     *  failed.
     */
    static final Object SpecialRetry = new Object();

    /** The number of spins before yielding. */
    static final int SpinCount = Integer.parseInt(System.getProperty("spin", "100"));

    /** The number of yields before blocking. */
    static final int YieldCount = Integer.parseInt(System.getProperty("yield", "0"));

    static final int OVLBitsBeforeOverflow = Integer.parseInt(System.getProperty("shrinkbits", "8"));

    // we encode directions as characters
    static final char Left = 'L';
    static final char Right = 'R';
    
    // Possible colors
    protected enum NodeColor { Red,Black,DoubleBlack };
//
//	static final boolean Red = false;
//	static final boolean Black = true;

    // return type for extreme searches
    static final int ReturnKey = 0;
    static final int ReturnEntry = 1;
    static final int ReturnNode = 2;
    
    // The root holder of the tree. 
    private final Node<K,V> rootHolder = new Node<K,V>(null,null,null,0L,NodeColor.Black,null,null);
    
    /** An <tt>OVL</tt> is a version number and lock used for optimistic
     *  concurrent control of some program invariant.  If {@link #isChanging}
     *  then the protected invariant is changing.  If two reads of an OVL are
     *  performed that both see the same non-changing value, the reader may
     *  conclude that no changes to the protected invariant occurred between
     *  the two reads.  The special value UnlinkedOVL is not changing, and is
     *  guaranteed to not result from a normal sequence of beginChange and
     *  endChange operations.
     *  <p>
     *  For convenience <tt>endChange(ovl) == endChange(beginChange(ovl))</tt>.
     */
    private static final long UnlinkedOVL = 1L;
    private static final long OVLGrowLockMask = 2L;
    private static final long OVLShrinkLockMask = 4L;
    private static final int OVLGrowCountShift = 3;
    private static final long OVLGrowCountMask = ((1L << OVLBitsBeforeOverflow) - 1) << OVLGrowCountShift;
    private static final long OVLShrinkCountShift = OVLGrowCountShift + OVLBitsBeforeOverflow;

    private static long beginGrow(final long ovl) {
      ////Assert.assertTrue(!isChangingOrUnlinked(ovl));
      return ovl | OVLGrowLockMask;
    }

    private static long endGrow(final long ovl) {
      //Assert.assertTrue(!isChangingOrUnlinked(ovl));

      // Overflows will just go into the shrink lock count, which is fine.
      return ovl + (1L << OVLGrowCountShift);
    }

    private static long beginShrink(final long ovl) {
      //Assert.assertTrue(!isChangingOrUnlinked(ovl));
      return ovl | OVLShrinkLockMask;
    }

    private static long endShrink(final long ovl) {
      //Assert.assertTrue(!isChangingOrUnlinked(ovl));

      // increment overflows directly
      return ovl + (1L << OVLShrinkCountShift);
    }

    private static boolean isChanging(final long ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask)) != 0;
    }
    private static boolean isUnlinked(final long ovl) {
        return ovl == UnlinkedOVL;
    }
    private static boolean isShrinkingOrUnlinked(final long ovl) {
        return (ovl & (OVLShrinkLockMask | UnlinkedOVL)) != 0;
    }
    private static boolean isChangingOrUnlinked(final long ovl) {
        return (ovl & (OVLShrinkLockMask | OVLGrowLockMask | UnlinkedOVL)) != 0;
    }

    private static boolean hasShrunkOrUnlinked(final long orig, final long current) {
        return ((orig ^ current) & ~(OVLGrowLockMask | OVLGrowCountMask)) != 0;
    }
    private static boolean hasChangedOrUnlinked(final long orig, final long current) {
        return orig != current;
    }
    
    /**
     * Returns color of Node<K,V> n. Black if n is null.
     * @param n
     * @return
     */
	private static NodeColor color(final Node<?,?> n) {
		return n == null ? NodeColor.Black : n.color;
	}
    
	private static class Node<K,V> {
		
		volatile Node<K,V> parent;
		volatile Node<K,V> left;
		volatile Node<K,V> right;
		
		final K key;
		volatile Object value;
		
		volatile NodeColor color;	
		volatile long changeOVL;
		
		Node(final K key, final Object value, final Node<K,V> parent, 
				final long version, final NodeColor color,final Node<K,V> leftSon, final Node<K,V> rightSon)
		{
			this.key = key;
			this.value = value;
			this.parent = parent;
			this.changeOVL = version;
			this.color = color;
			this.left = leftSon;
			this.right = rightSon;
		}
		
		Node<K,V> child(char dir){   return dir == Left ? left : right ;}
		Node<K,V> sibling(char dir){ return dir == Left ? right : left;}
		
		void setChild(char dir, Node<K,V> n){
			if(dir == Left)
				left = n;
			else
				right = n;
		}
		
	    //////// per-node blocking

	    void waitUntilChangeCompleted(final long ovl) {
	        if (!isChanging(ovl)) {
	            return;
	        }

	        for (int tries = 0; tries < SpinCount; ++tries) {
	            if (changeOVL != ovl) {
	                return;
	            }
	        }

	        for (int tries = 0; tries < YieldCount; ++tries) {
	            Thread.yield();
	            if (changeOVL != ovl) {
	                return;
	            }
	        }

	        // spin and yield failed, use the nuclear option
	        synchronized (this) {
	            // we can't have gotten the lock unless the shrink was over
	        }
	        //Assert.assertTrue(changeOVL != ovl);
	    }
	    
// Test Functions ------------------------------------------------------------------------------------------------------------------------------------------------

	    @Override
	    public String toString()
	    {
			if(value != null)
			{
				if(color == NodeColor.Black) {
					return ("[" + key + "]("+value+")");
				}
				else if(color == NodeColor.Red){
					return (key + "("+value+")");
				}
				else {
					return (key + "{" + value + "}");
				}
			}
			else
			{
				if(color == NodeColor.Black) {
					return ("*[" + key + "]*("+value+")");
				}
				else if (color == NodeColor.Red){
					return "*" + key + "*("+value+")";
				}
				else {
					return ("*{" + key + "}*("+value+")");
				}
					
			}
	    }
	    
		public void printNodeKey(int height, int id)
		{
			try {
				if(right != null) right.printNodeKey(height+1,id);
				
				if(value != null)
				{
					if(color == NodeColor.Black)
					{
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + "[" + key + "](" +value+"-"+ id +")");
					}
					else if(color == NodeColor.Red)
					{
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + key + "("+value+"-"+ id +")");
					}
					else {
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + "{" + key + "}(" +value+"-"+ id +")");
					}
						
				}
				else
				{
					if(color == NodeColor.Black)
					{
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + "*[" + key + "]*(" +value+"-"+ id +")");
					}
					else if(color == NodeColor.Red)
					{
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + "*" + key + "*("+value+"-"+ id +")");
					}
					else {
						String tabs = "";
						for(int i = 0; i < height; i++)
							tabs+= "\t";
						System.out.println(tabs + "*{" + key + "}*(" +value+"-"+ id +")");
					}
				}
				
				if(left != null) left.printNodeKey(height+1,id);
			
			} catch (Exception e) {
				String tabs = "";
				for(int i = 0; i < height; i++)
					tabs+= "\t";
				System.out.println(tabs + key + "(ERROR)");
			}
		}

	} // End of definition of Node<K,V>.
	
	// Node<K,V> access
	
    @SuppressWarnings("unchecked")
    private V decodeNull(final Object vOpt) {
        //Assert.assertTrue (vOpt != SpecialRetry);
        return vOpt == SpecialNull ? null : (V)vOpt;
    }

    private static Object encodeNull(final Object v) {
        return v == null ? SpecialNull : v;
    }
    
    private static final int UpdateAlways = 0;
    private static final int UpdateIfAbsent = 1;
    private static final int UpdateIfPresent = 2;
    private static final int UpdateIfEq = 3;

	public static final Object NULL = null;

    private static boolean shouldUpdate(final int func, final Object prev, final Object expected) {
        switch (func) {
            case UpdateAlways: return true;
            case UpdateIfAbsent: return prev == null;
            case UpdateIfPresent: return prev != null;
            default: return prev == expected; // TODO: use .equals
        }
    }
    
    private Comparator<? super K> comparator;

	// Public interface
	
	public ConcurrentRBTree()
	{}
	
    public ConcurrentRBTree(final Comparator<? super K> comparator) {
        this.comparator = comparator;
    }
	
    public boolean isEmpty() {
        // removed-but-not-unlinked nodes cannot be leaves, so if the tree is
        // truly empty then the root holder has no right child
    	
        return rootHolder.left == null;
    }
    
    public void clear() {
        synchronized (rootHolder) {
            rootHolder.left = null;
        }
    }
    
    // Busqueda
    
    @Override
    public V get(final Object key) {
        return decodeNull(getImpl(key));
    }
    
    private Object getImpl(final Object key) {
        final Comparable<? super K> k = comparable(key);
    	while(true)
    	{
    		final Node<K,V> n = rootHolder.left;
    		if(n == null)
    			return null;
    		final long v = n.changeOVL;
    		final int cDir = k.compareTo(n.key);
    		if(cDir == 0)
    		{
    			return n.value;
    		}
    		
    		if(isShrinkingOrUnlinked(n.changeOVL))
    			n.waitUntilChangeCompleted(n.changeOVL);
    		else if(n == rootHolder.left)
    		{
    				final Object result = attemptGet(k,n,v, cDir < 0 ? Left : Right);
    				if(result != SpecialRetry)
    					return result;    			
    		}
    	}
    }
    
    @SuppressWarnings("unchecked")
    private Comparable<? super K> comparable(final Object key) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (comparator == null) {
            return (Comparable<? super K>)key;
        }
        return new Comparable<K>() {
            final Comparator<? super K> _cmp = comparator;

            public int compareTo(final K rhs) { return _cmp.compare((K)key, rhs); }
        };
    }

	private Object attemptGet(final Comparable<? super K> k,
			final Node<K,V> n, 
			final long origOVL, 
			final char dir) {

		while(true)
		{
			// My current child
			final Node<K,V> child = n.child(dir);
            if (child == null) {
                if (hasShrunkOrUnlinked(n.changeOVL, origOVL)) {
                    return SpecialRetry;
                }

                // Node<K,V> is not present.  Read of node.child occurred while
                // parent.child was valid, so we were not affected by any
                // shrinks.
                return null;
            }
            else
            {	
            	final int cDir = k.compareTo(child.key);
            	
            	if(cDir == 0)
            		return child.value;
				
				long childOVL = child.changeOVL;
				// Wait till not changing
				if(isShrinkingOrUnlinked(child.changeOVL))
				{
					child.waitUntilChangeCompleted(child.changeOVL);
					
					// Check again
                    if (hasShrunkOrUnlinked(origOVL, n.changeOVL)) {
                        return SpecialRetry;
                    }
                    // Back to the beginning 
				}
				else if(child != n.child(dir))
				{
					//Try again
                    if (hasShrunkOrUnlinked(origOVL, n.changeOVL)) {
                        return SpecialRetry;
                    }
				}
				else // child == n.child(dir)
				{
                    if (hasShrunkOrUnlinked(origOVL, n.changeOVL)) {
                        return SpecialRetry;
                    }
					Object result = attemptGet(k, child, childOVL, cDir < 0 ? Left : Right);
					if(result != SpecialRetry)
						return result;
				}
            }
		}
		
	}
	
	// Updates
	
	@Override
	public V put(final K key, final V value)
	{
		return decodeNull(update(key,encodeNull(value)));
	}
	
	private static final int UnlinkRequired = -1;
    private static final int RebalanceRequired = -2;
    private static final int NothingRequired = -3;
    private static final int CheckParent = -4;

	private int nodeCondition(final Node<K,V> n, final boolean unlinkPossible) {
		
		// Begin atomic
		final Node<K,V> nL = n.left;
		final Node<K,V> nR = n.right;
		
		// Unlink possibility we do not check all of them.
		// The case we do not check is when n is a black node
		// with no sons. This case should not appear often (as these are insertions.)
		if(unlinkPossible && (n.value == null && (nL == null || nR == null)) )
		{
			return UnlinkRequired;
		}
		
		final NodeColor cN = color(n);
		final NodeColor cL0 = color(nL);
		final NodeColor cR0 = color(nR);
		
		// End atomic
		
		if(cN !=  NodeColor.Red)
			return NothingRequired;
		else if(cL0 == NodeColor.Red || cR0 == NodeColor.Red)
			return RebalanceRequired;
		else
			return CheckParent;
	}
	
	private boolean attemptRemoveRouteNode(final Node<K,V> n)
	{
		removeNode(n);

		// We succeeded in removing the node if it is unlinked.
		if(isUnlinked(n.changeOVL)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	private void fixColorAndRebalance(Node<K,V> n) {
		while(n != null && n.parent != null)
		{
			int condition = nodeCondition(n, true);
			
			if(condition == UnlinkRequired)
			{
				if(attemptRemoveRouteNode(n)) {
					// Finished
					return;
				}	
				else {
					// We need a different condition...
					condition = nodeCondition(n, false);
				}
			}
			
            if (condition == NothingRequired || isUnlinked(n.changeOVL)) {
                // nothing to do, or no point in fixing this node
                return;
            }
            else if(condition == RebalanceRequired)
            {
            	n = rotateOrPushRed(n);
            }
            else if(condition == CheckParent) // Check parent
            { 
            	
            	if(n.parent == rootHolder)
            	{
                	synchronized (rootHolder) {
                		synchronized (n) {
                			if(n.parent == rootHolder && !isUnlinked(n.changeOVL)) // We are the root. In which case we should blacken the root.
                			{
                				n.color = NodeColor.Black;
                				return; // Done!
                			}
                			else
                			{
                				n = n.parent;
                			}
                		}
                	}
            	}
            	else {
            		synchronized (n) {
            			if(n.color == NodeColor.Red && (color(n.left) == NodeColor.Red || color(n.right) == NodeColor.Red) ) {
            				continue;
            			}
            			
						if(n.parent == rootHolder) {
							continue;
						}
						else {
							n = n.parent;
						}
					}
            	}

            }
		}
	}
	
    //////////////// tree balance and color info repair
	
    /** rotation needed around node n. Need to get grandfather (gp) and father and n. 
     */
    private Node<K,V> rotateOrPushRed(final Node<K,V> n) {

    	final Node<K,V> p = n.parent; // Parent
    	final Node<K,V> g = p.parent; // Grandparent
    	
    	if(g == null)
    	{
    		// Blacken root ( g is null --> p is rootHolder --> n is root)
    		synchronized (p) {
    			if (n.parent == p) {
    				synchronized (n) {
    					if(p == rootHolder && !isUnlinked(n.changeOVL))
    					{
    						n.color = NodeColor.Black;
    						return null;
    					}
    					else
    					{
    						return n; // Retry same node
    					}
					}
    			}
    			else
    			{
    				return n; // Retry same node
    			}
			}
    	}
		else if (color(p) == NodeColor.Red) {
			fixColorAndRebalance(p);
			return n;
		}
    	else
    	{
	    	synchronized (g) {
	    		if (!isUnlinked(g.changeOVL) && p.parent == g) {
	    			synchronized (p) {
	    	    		if (!isUnlinked(p.changeOVL) && n.parent == p) {
	    	    			// Link g -> p -> n is locked. Also Link p -> s is also locked.
	    	    			// Colors of g,p,n,s are also locked. 
	    	    			final NodeColor nColor = color(n);
	    	    			final NodeColor pColor = color(p);
	    	    			
	    	    			if(nColor!= NodeColor.Red)
	    	    			{
	    	    				return n; // Retry
	    	    			}
	    	    			
	    	    			if (pColor == NodeColor.Red) {
	    	    				// We have a red - red - red conflict. p,n and at least one of
	    	    				// n's children
	    	    				return n;
	    	    			}
	    	    			
	    	    			// Push red If I Push red then I correct the red - red conflict. 
	    	    			if(color(p.left) == NodeColor.Red && color(p.right) == NodeColor.Red)
		    				{
		    					// We have the locks necessary to push red.
    	    					p.left.color = NodeColor.Black;
    	    					p.right.color = NodeColor.Black;
	    	    				if(pColor == NodeColor.DoubleBlack)
	    	    				{
	    	    					// We have eliminated the doubleBlackness
	    	    					p.color = NodeColor.Black;
	    	    					return n;
	    	    				}
	    	    				else if(pColor == NodeColor.Black)
	    	    				{
	    	    					// Push red
									p.color = NodeColor.Red;
									if(g != rootHolder) {
										return g; // Damaged g
									}
									else {
										return p;
									}
	    	    				}
	    	    				else {
	    	    					//Assert.assertTrue(false);
	    	    					return n;
	    	    				}
		    				}
		    				else // Rotate
		    				{
		    					// Things we know going in:
		    					// n is red 
		    					// p is black or double black
		    					// s is black.
		    					return rebalance_nl(g, p,n);
		    				}
	    	    		}
					}
	    		}
			}
    	// The path to g was interrupted at some point. Try again.
    	return n;
    	}
	}


    /**
     * Rotates n around p. Both g and p must be locked on entry. One and only one of p.Left or p.Right must be black
     *  The other son is n and is a red node with a red red conflict with one of his sons.
     *	Return a damaged node or null if no more rebalancing is necessary
     * @param g Locked
     * @param p Locked AND p is Black or DoubleBlack
     * @param n is Red
     * @return damaged node
     */
    private Node<K,V> rebalance_nl(final Node<K,V> g, final Node<K,V> p, final Node<K,V> n)
    {    	
    	if(p.left == n )
    		return rebalance_to_Right(g,p,n);
    	else if(p.right == n)
    		return rebalance_to_Left(g,p,n);
    	else
    		return n; // Retry
    }
    
	private Node<K,V> rebalance_to_Left(
			Node<K,V> g,
			Node<K,V> p,
			Node<K,V> n) {
		
		synchronized (n) {
			final Node<K,V> nL = n.left;
			final Node<K,V> nR = n.right;
			
			final NodeColor nLcolor = color(nL);
			final NodeColor nRcolor = color(nR);
			final NodeColor pColor = p.color;
			
			if(nLcolor != NodeColor.Red && nRcolor != NodeColor.Red) 
			{
				// Retry there is no unbalance probably due to a rebalance lower in the tree
				return n;
			}
			else if(nLcolor == NodeColor.Red && nRcolor == NodeColor.Red)
			{
				// We must rotate and then push red
				rotateLeft_nl(g, p, n, nL);
				if(pColor == NodeColor.DoubleBlack)
				{
					// Everything is made black
					n.color = NodeColor.Black;
					nR.color = NodeColor.Black;
					p.color = NodeColor.Black;
					return p;	
				}
				else
				{
					//n.color = NodeColor.Red;
					nR.color = NodeColor.Black;
					//p.color = NodeColor.Black; // Redundant, p was already Black
					if(g != rootHolder) {
						return g;
					}
					else {
						return n;
					}
				}
			}
			// We prefer a simple rotation to a double rotation
			else if(nRcolor == NodeColor.Red)
			{
				rotateLeft_nl(g, p, n, nL);
				// fix colors
				if(pColor == NodeColor.DoubleBlack)
				{
					n.color = NodeColor.Black;
					nR.color = NodeColor.Black;
					p.color = NodeColor.Black;
				}
				else
				{
					n.color = NodeColor.Black;
					p.color = NodeColor.Red;
					//nR.color = NodeColor.Red; Redundant
				}
				return p;
			}
			else // Double rotation color(nR) = Red
			{
				synchronized (nL) {
					rotateLeftOverRight_nl(g, p, n, nL);
					// Fix colors
					if(p.color == NodeColor.DoubleBlack) {
						nL.color = NodeColor.Black;
						n.color = NodeColor.Black;
						p.color = NodeColor.Black;
						return p;
					}
					else if(nLcolor == NodeColor.Red || nRcolor == NodeColor.Red)
					{
						// We have to push red
						//nL.color = NodeColor.Red;  Redundant
						n.color = NodeColor.Black;
						//p.color = NodeColor.Black; Redundant
						if(g != rootHolder) {
							return g;
						}
						else {
							return nL;
						}
					}
					else {
						nL.color = NodeColor.Black;
						p.color = NodeColor.Red;
						//n.color = NodeColor.Red; Redundant
						return p;
					}
				}
			}
		}
	}

	private Node<K,V> rebalance_to_Right(
			final Node<K,V> g,
			final Node<K,V> p,
			final Node<K,V> n) {
		
		synchronized (n) {
			
			final Node<K,V> nL = n.left; 
			final Node<K,V> nR = n.right;
			
			final NodeColor nLcolor = color(nL);
			final NodeColor nRcolor = color(nR);
			final NodeColor pColor = p.color;
			
			if( (nLcolor != NodeColor.Red && nRcolor != NodeColor.Red)) {
				return n;
			}
			else if(nLcolor == NodeColor.Red && nRcolor == NodeColor.Red)
			{
				rotateRight_nl(g, p, n, nR);
				// Rotate and push red
				if(pColor == NodeColor.DoubleBlack)
				{
					p.color = NodeColor.Black;
					n.color = NodeColor.Black;
					nL.color = NodeColor.Black;
					return p;
				}
				else 
				{
					//n.color = NodeColor.Red; // Redundant
					nL.color = NodeColor.Black;
					//p.color = NodeColor.Black; // Redundant
					if(g != rootHolder) {
						return g;
					}
					else {
						return n;
					}
				}
			}
			else if(nLcolor == NodeColor.Red) 
			{
				//Assert.assertTrue(color(nR) != NodeColor.Red);
				rotateRight_nl(g, p, n, nR);
				// Left-Left Situation. We rotate right.
				// fix colors
				if(pColor == NodeColor.DoubleBlack) {
					n.color = NodeColor.Black;
					nL.color = NodeColor.Black;
					p.color = NodeColor.Black;
				}
				else {
					n.color = NodeColor.Black;
					p.color = NodeColor.Red;
					//nL.color = NodeColor.Red; Redundant
				}
				return p;
			}
			else // color(nR) == Red
			{
				// Left-Right Situation. Double rotation
				synchronized (nR) {
					rotateRightOverLeft_nl(g, p, n, nR);
					//Fix colors
					if(pColor == NodeColor.DoubleBlack) {
						nR.color = NodeColor.Black;
						n.color = NodeColor.Black;
						p.color = NodeColor.Black;
						return p;
					}
					else if(nRcolor == NodeColor.Red || nLcolor == NodeColor.Red)
					{
						// We must rotate and then push red
						// nR.color = NodeColor.Red; Redundant
						n.color = NodeColor.Black;
						// p.color = NodeColor.Black; Redundant
						if(g != rootHolder) {
							return g;
						}
						else {
							return nR;
						}
					}
					else {
						//n.color = NodeColor.Red; Redundant
						nR.color = NodeColor.Black;
						p.color = NodeColor.Red;
						return p;
					}
				}
			}
		}	
	}

	@Override
    public V remove(final Object key) {
        return decodeNull(update(key, null));
    }
	
	private Object update(final Object key, final Object value)
	{
		final Comparable<? super K> k = comparable(key);
		while(true)
		{
			Node<K,V> root = rootHolder.left;
			if(root == null)
			{
				if(value == null || attemptInsertIntoEmpty(key,value))
					return null;
			}
			else
			{
				long rootOVL = root.changeOVL;
				
				if(isShrinkingOrUnlinked(rootOVL))
					root.waitUntilChangeCompleted(rootOVL);
				else if (root == rootHolder.left)
				{
					Object oldValue = attemptUpdate(key,k,value,rootHolder, root, rootOVL);
					if(oldValue != SpecialRetry)
						return oldValue;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean attemptInsertIntoEmpty(final Object key, final Object value) {	
		synchronized (rootHolder) {
			if(rootHolder.left == null)
			{
				rootHolder.left = new Node<K,V>((K)key, value, rootHolder, 0L, NodeColor.Black, null, null);
				return true;
			}
			else
				return false;		
		}
	}

	@SuppressWarnings("unchecked")
	private Object attemptUpdate(final Object key,
            final Comparable<? super K> k,
			final Object value,
			final Node<K,V> parent,
			Node<K,V> n, 
			final long nOVL) {
		
		//Assert.assertTrue(nOVL != UnlinkedOVL);
		
		final int nComp = k.compareTo(n.key);
		if(nComp == 0)
		{
			return attemptUpdateNode(key,value,n, parent);
		}
		final char cDir = nComp < 0 ? Left : Right;
		
		while(true)
		{
			final Node<K,V> child = n.child(cDir);
			// Proceed to child
			if(hasShrunkOrUnlinked(nOVL, n.changeOVL))
				return SpecialRetry;
	
			if(child == null)
			{
				if(value == null)
				{
					// Nothing to remove.
					return null;
				}
				
				final boolean success;
				final Node<K,V> damaged;
				synchronized (n) {
					// Affected go back to parent
					if(hasShrunkOrUnlinked(nOVL, n.changeOVL))
					{
						return SpecialRetry;
					}

					if(n.child(cDir) != null)
					{
						// Somebody already inserted just retry
						success = false;
						damaged = null;
					}
					else
					{
						// value != null
						n.setChild(cDir, new Node<K,V>((K)key,value,n,0L,NodeColor.Red,null,null));
						success = true;
						damaged = n;
					}
				}
				if(success)
				{
					fixColorAndRebalance(damaged);
					return null;
				}
				// Retry
			}
			else
			{
				final long childOVL = child.changeOVL;
				if(isChanging(childOVL))
				{
					child.waitUntilChangeCompleted(childOVL);
					// Retry
				}
				else if(!(n.child(cDir) == child))
				{
					// Retry
				}
				else
				{
					if(hasShrunkOrUnlinked(nOVL, n.changeOVL))
						return SpecialRetry;
					// Ok carry on
					Object oldValue = attemptUpdate(key,k, value, n, child, childOVL);
					if(oldValue != SpecialRetry)
						return oldValue;
				}
			}
		}		
	}

	private Object attemptUpdateNode(final Object key, final Object newValue,final Node<K,V> n,final Node<K,V> parent) {
        if (newValue == null) {
            // removal
            if (n.value == null) {
                // This node is already removed, nothing to do.
                return null;
            }
            final Object oldValue;
			synchronized (n) {
				if(isUnlinked(n.changeOVL))
					return SpecialRetry;
				// Make route node then try to remove
				oldValue = n.value;				
				n.value = null;
			}
			if(n.left == null || n.right == null) {
				removeNode(n);
			}
			
			return oldValue;
        }
		else
		{
			synchronized (n) {
				if(isUnlinked(n.changeOVL))
					return SpecialRetry;
				final Object oldValue = n.value;
				n.value = newValue;
				return oldValue;
			}
		}
	}
	
	private void removeNode(final Node<K,V> n)
	{
		// We stop on 5 possible conditions:
		// 	1 n.value != null... somebody just inserted n
		//  2 isUnlinked(n.changeOVL) somebody managed to remove n
		// 	3 n has 2 sons, in which case we can't remove
		// 	4 n is a double black node. We can't remove him
		//  5 we manage to remove a n. This happens inside the loop and it returns.
		while( (n.left == null || n.right == null) && n.value == null && !isUnlinked(n.changeOVL))
		{			
			final NodeColor nColor = n.color;
			
			if(nColor == NodeColor.Red)
			{
				if(attemptRemoveRedNode(n))
					break;
			}
			else if(nColor == NodeColor.Black)
			{
				if(attemptRemoveBlackNode(n))
					break;
			}
			else { // nColor is double black and can't be removed
				break;
			}
		}
	}
	
	private boolean attemptRemoveRedNode(final Node<K,V> n)
	{		
			final Node<K,V> parent = n.parent;
			final Node<K,V> damaged;
			synchronized (parent) {
				if(n.parent != parent || isUnlinked(parent.changeOVL))
				{
					return false;
				}
				
				synchronized (n) {
					if(isUnlinked(n.changeOVL) || n.value != null || (n.left != null && n.right != null)) {
						return false;
					}
					
					if(n.color == NodeColor.Red)
					{
						damaged = removeRedNode(parent, n);
					}
					else // Try again
					{
						return false;
					}
				} // Synchronized n
			} // Synchronized parent
			if(damaged != null) {
				fixColorAndRebalance(damaged);
			}
			return true;
	}
	
/**
 * Removes a red node. If both sons are null we remove the node, if one son is not null we splice the node 
 * n Can't have 2 sons!
 * @param parent Locked
 * @param n Locked AND n is Red AND n is not double black
 * @return Return a possibly damaged node.
 */
	private Node<K,V> removeRedNode(final Node<K,V> parent, final Node<K,V> n)
	{
		//Assert.assertTrue(Thread.holdsLock(n));
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue(n.left == null || n.right == null);
		
		final Node<K,V> damaged;
		if(n.left == null && n.right == null)
		{
			removeSonLessNode(parent, n);
			damaged = parent;
		}
		else // Has one son
		{
			final Node<K,V> son = n.left != null ? n.left : n.right;
			if(n == parent.left) {
				parent.left = son;
			}
			else {
				parent.right = son;
			}
			son.parent = parent;
			damaged = son;

			n.changeOVL = UnlinkedOVL;
		}
		return damaged;
	}
	
	/***
	 * Remove the root n
	 * @param n
	 * @param parent Must be rootHolder or we return false
	 * @return true if successful, false otherwise
	 */
	private boolean attemptRemoveRoot(final Node<K,V> n, final Node<K,V> parent)
	{
		synchronized (parent) {
			synchronized (n) {	
				if(n.value != null || isUnlinked(n.changeOVL)) {
					return false;
				}
				
				if( n.left != null && n.right != null) {
					return false;
				}
				
				if(n.parent != parent || n.parent != rootHolder)
				{
					return false;
				}
				else // Remove root:  root is n and rootHolder is parent
				{
					removeRoot(n, parent);
				}
			} // Synchronized n
		} // Synchronized parent
		return true;
	}
	
	/***
	 * Removed root n  
	 * parent must be rootHolder
	 * @param parent Locked
	 * @param root Locked has 0 sons or 1 son
	 */
	private void removeRoot(final Node<K,V> root,final Node<K,V> parent)
	{
		//Assert.assertTrue(parent == rootHolder);
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue(Thread.holdsLock(root));
		
		final Node<K,V> rootR = root.right;
		final Node<K,V> rootL = root.left;

		if(root.color != NodeColor.Black) {
			root.color = NodeColor.Black;
		}
		
		if(rootL == null && rootR == null)
		{
			// Remove node
			removeSonLessNode(parent, root);
		}
		else // Has only one son.
		{
			//Assert.assertTrue(!(color(rootL) == NodeColor.Black && color(rootR) == NodeColor.Black));
			
			// Replace root with only red son
			final Node<K,V> splice = rootL != null ? rootL : rootR;

			// parent is rootHolder (left son is root)
			parent.left = splice;
			splice.parent = parent;
			splice.color = NodeColor.Black;

			//Assert.assertTrue(Thread.holdsLock(rootHolder));
			//Assert.assertTrue(Thread.holdsLock(root));
			
			root.changeOVL = UnlinkedOVL;
		}
	}
	
	/**
	 * Attempts to remove the node n.
	 * @param n The node to be removed
	 * @return true if successful, false otherwise
	 */
	private boolean attemptRemoveBlackNode(final Node<K,V> n)
	{
		final Node<K,V> parent = n.parent;
		if(parent == null) {
			return false;
		}
		final Node<K,V> g = parent.parent;
		if(g == null) {
			return attemptRemoveRoot(n, parent);
		}

		// n is not the root node
		if(n.left == null && n.right == null)
		{
			return attempRemoveBlackLeaf(n, parent, g);
		}
		else if( n.left == null || n.right == null)
		{
			return attemptSpliceBlackNode(parent, n);
		}
		return false;
	}
	
	
	
	private boolean attemptSpliceBlackNode(Node<K, V> parent, Node<K, V> n) {
		
		final Node<K,V> damaged;
		
		synchronized (parent) {
			if(n.parent != parent || isUnlinked(parent.changeOVL))
			{
				return false;
			}
			// Link p -> n is locked. Also Link p -> s is also locked.
			// Colors of p,n,s are also locked. 
			synchronized (n) 
			{		
				final NodeColor nColor = n.color;
				
				if(n.value != null || nColor == NodeColor.DoubleBlack || isUnlinked(n.changeOVL)) {
					return false;
				}
				
				final Node<K,V> nL = n.left;
				final Node<K,V> nR = n.right;
				
				// Have 0 or 2 sons
				if( (nL == null && nR == null) || (nL != null && nR != null) ) {
					return false;
				}
				
				if(nColor == NodeColor.Red) {
					damaged = removeRedNode(parent, n);
				}
				else 
				{
					damaged = nL != null ? nL : nR;
					spliceBlackNode(parent, n);
				}
			}
		}
		
		fixColorAndRebalance(damaged);
		
		return false;
	}
	
	/***
	 * Splices n with it's only son which must be red.
	 * n and parent must be locked on entry.
	 * Return true if splice was successful.
	 * You should save n's oldValue before calling.
	 * @param parent locked
	 * @param n locked AND normalized
	 * @return true if successfully splices n.
	 */
	private void spliceBlackNode(final Node<K,V> parent, final Node<K,V> n)
	{
	//	System.out.println(Thread.currentThread().getName() + " tried to splice node" + n.key);
		//Assert.assertTrue(Thread.holdsLock(n));
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue( (n.left != null && n.right == null) || (n.left == null && n.right != null));
		//Assert.assertTrue(n.color == NodeColor.Black);

		final Node<K,V> son = n.left != null ? n.left : n.right;
		
		//Assert.assertTrue(son.color == NodeColor.Red);
		
		// Replace with only red son
		if(parent.left == n) {
			parent.left = son;
		}
		else {
			parent.right = son;
		}
		son.parent = parent;
		son.color = NodeColor.Black;
		
		n.changeOVL = UnlinkedOVL;
	}

	/***
	 * Attempts to remove n, which is a black leaf
	 * n, parent and g are not null
	 * @param n
	 * @param parent
	 * @param g
	 * @return
	 */
	private boolean attempRemoveBlackLeaf(final Node<K,V> n,final Node<K,V> parent, final Node<K,V> g)
	{
		final Node<K,V> damaged;
		Node<K,V> doubleBlackNode = null; // Marks a double black node which is created
		
		// n has two null children

		synchronized (g) {
			if(parent.parent != g || isUnlinked(g.changeOVL)) {
				return false;
			}
			synchronized (parent) {
				if(n.parent != parent || isUnlinked(parent.changeOVL))
				{
					return false;
				}
    			// Link g -> p -> n is locked. Also Link p -> s is also locked.
    			// Colors of g,p,n,s are also locked. 
				synchronized (n) 
				{				
					final NodeColor nColor = n.color;
					final Node<K,V> nL = n.left;
					final Node<K,V> nR = n.right;
					
					if(n.value != null || nColor == NodeColor.DoubleBlack || isUnlinked(n.changeOVL)) {
						return false;
					}
					
					if(nL != null && nR != null)
					{
						return false;
					}
										
					if(nColor == NodeColor.Red)
					{
						damaged = removeRedNode(parent, n);
						doubleBlackNode = null;
					}
					// g,p,n locked
					// n is a black node with 0 or 1 son
					// n is not double black					
					else if(nL != null || nR != null) // Only one null son
					{
						final Node<K,V> son = nL != null ? nL : nR;
						
						spliceBlackNode(parent, n);
						damaged = son; // NEW was null
						doubleBlackNode = null;
					} // Finished one son case
					else
					{
						// n is a black leaf
						final char nDir = parent.left == n ? Left : Right;
						
						//Assert.assertTrue(parent.child(nDir) == n);
						
						final Node<K,V> s = parent.sibling(nDir);
						
						if(s==null)
							System.out.println("Oops...");
						
						//Assert.assertTrue(s != null);
						
						synchronized (s) {
							final Node<K,V> sL = s.left;
							final Node<K,V> sR = s.right;
							
							final Node<K,V>[] result;
							if(s.color == NodeColor.Red) {
								result = resolveRedSibling(g, parent, s, sL, sR, nDir);
							} // Finished s is red
							else {
								result = resolveBlackNode(g, parent, s, nDir); // Solved both black and double black cases
							}
							if(result == null) {
								return false;
							}
							damaged = result[0];
							doubleBlackNode = result[1];
							
							// We have finished all cases if we got here it means that we can remove the node					
							// Remove n. By removing at the end we avoid any situation where there is clearly a imbalance of black nodes.
							// There may still be a case where there is a black node imbalance (in Push black's case) but it is not apparent
							// to the other threads.
							removeSonLessNode(parent, n);
						} // Synchronization of s
						
					}// n is a leaf					
				} // Synchronization n
			} // Synchronization parent
		} // Synchronization g
		
		if(damaged != null) {
			fixColorAndRebalance(damaged);
		}
		
		if(doubleBlackNode != null) {
			correctBlackCount(doubleBlackNode);
		}
		return true;
	}
	
	private Node<K,V>[] resolveRedSibling(final Node<K,V> g, final Node<K,V> parent, final Node<K,V> s,final Node<K,V> sL, final Node<K,V> sR, final char nDir)
	{
		//Assert.assertTrue(s.parent == parent);
		//Assert.assertTrue(!isUnlinked(s.changeOVL));
		
		// Case 1 falls into other cases
		final Node<K,V> newS = s.child(nDir); // newS is inner child of s
		
		//Assert.assertTrue(newS != null);
		
		if(newS.color == NodeColor.Red) {
			 // No sense continuing TODO: Should be exponential back off
			return null;
		}
			
		synchronized (newS) {
			//Assert.assertTrue(newS.parent == s);
			//Assert.assertTrue(!isUnlinked(newS.changeOVL));
															
			if( !case1(g, parent, s,sL,sR, nDir) ) {
				// Failed
				return null;
			}
			
			//Assert.assertTrue(parent.sibling(nDir) == newS);
			//Assert.assertTrue(parent.parent == s);
			//Assert.assertTrue("newS was red...", color(newS) != NodeColor.Red);

			// We know newS is black
			return resolveBlackNode(s, parent, newS, nDir);
		} // Synchronization newS
	}
	/***
	 * Resolves a black node by rotating or pushing black
	 * @param g Must be locked
	 * @param parent Must be locked, if double black we can't resolve n
	 * @param s Must be locked, can't be red
	 * @param nDir
	 * @return an array of nodes := { damaged, doubleBlackNode } or null if we could not resolve n
	 */
	@SuppressWarnings("unchecked")
	private Node<K,V>[] resolveBlackNode(final Node<K,V> g, final Node<K,V> parent, final Node<K,V> s, final char nDir)
	{
		//Assert.assertTrue(Thread.holdsLock(g));
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue(Thread.holdsLock(s));
		//Assert.assertTrue(Thread.holdsLock(parent.left) && Thread.holdsLock(parent.right));
		
		//Assert.assertTrue(s.color != NodeColor.Red);
		
		final Node<K,V> doubleBlackNode;
		final Node<K,V> damaged;
		
		// s is Black or double black
		final Node<K,V> sL = s.left;
		final Node<K,V> sR = s.right;
		
		final NodeColor pColor = parent.color;
		final NodeColor sLColor = color(sL);
		final NodeColor sRColor = color(sR);

		if(s.color == NodeColor.DoubleBlack) {
			if(pColor != NodeColor.DoubleBlack)
			{
				s.color = NodeColor.Black;
				// Push black
				if(pColor == NodeColor.Red)
				{
					parent.color = NodeColor.Black;
					damaged = parent;
					doubleBlackNode = null; 
				}
				else 
				{
					parent.color = NodeColor.DoubleBlack;
					damaged = s;
					doubleBlackNode = parent; // Pushed black onto black parent
				}
			}
			else {
				// We can't push black onto an already double black node
				return null;
			}
			
			//Assert.assertTrue(s.color == NodeColor.Black);
			//Assert.assertTrue( (c == NodeColor.Black && parent.color == NodeColor.DoubleBlack) ||
			//				   (c == NodeColor.Red && parent.color == NodeColor.Black) );
		}
		else if(sLColor != NodeColor.Red && sRColor != NodeColor.Red) // Case 2
		{			
			// Push Black
			if(pColor != NodeColor.DoubleBlack)
			{
				s.color = NodeColor.Red;
				if(pColor == NodeColor.Black) 
				{
					doubleBlackNode = parent;
					parent.color = NodeColor.DoubleBlack;
				}
				else // parent.color is red no sense propagating changes
				{
					parent.color = NodeColor.Black;
					doubleBlackNode = null;
				}
				
				if(s.value == null && sL == null && sR == null) {
					// We can remove s freely
					removeSonLessNode(parent, s);
					damaged = parent;
				} 
				else {
					damaged = s;
				}
			}
			else {
				return null;
			}
		}
		else // Case 3 and 4
		{
			doubleBlackNode = rebalanceSibling(g, parent, s, sL, sR, nDir);
			damaged = s;    // necessary to make sure that complicated red red cases don't fail
		}
		
		@SuppressWarnings("rawtypes")
		Node[] result = {damaged, doubleBlackNode};
		return result;
	}
	



	
	/**
	 * Case 1: P is black and s is Red
	 * @param g Locked
	 * @param parent Locked Normalized, p.DB = 0 or 1
	 * @param s Locked, is Red, s.DB = 0
	 * @param sL normalized
	 * @param sR normalized
	 * @param nDir Left if n is left son of parent
	 * @return true if successful. 
	 */
	private boolean case1(final Node<K,V> g, final Node<K,V> parent, final Node<K,V> s,final Node<K,V> sL, final Node<K,V> sR, final char nDir)
	{				
		//Assert.assertTrue(Thread.holdsLock(g));
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue(Thread.holdsLock(s));
		//Assert.assertTrue(Thread.holdsLock(s.child(nDir)));
		//Assert.assertTrue(s.color == NodeColor.Red);
		
		final NodeColor pColor = parent.color;
		final NodeColor sLColor = color(sL);
		final NodeColor sRColor = color(sR);
		
		if(nDir == Left) {
			
			if(sLColor == NodeColor.Red)
			{
			//	this.redProblem.incrementAndGet();
				return false;
			}
			else if(pColor == NodeColor.DoubleBlack)
			{
				if( sRColor == NodeColor.Black){
					return false;
				}
				else {
					// Rotate and paint everything Black
					parent.color = NodeColor.Black;
					s.color = NodeColor.Black;
					sR.color = NodeColor.Black;
					rotateLeft_nl(g, parent, s, sL);
				}
			}
			else if(pColor == NodeColor.Red)
			{
				return false;
				// Rotate and don't paint anything... lots of red! :) lots of problems :(
				//rotateLeft_nl(g, parent, s, sL);
			}
			// parent is black 
			else
			{
				// Simple case sL is black so we simply rotate
				s.color = NodeColor.Black;
				parent.color = NodeColor.Red;
				rotateLeft_nl(g, parent, s, sL);
			}
		}
		else {
			if(sRColor == NodeColor.Red)
			{
				return false;
			}
			else if(pColor == NodeColor.DoubleBlack)
			{
				if(sLColor == NodeColor.Black) {
					return false;
				}
				else {
					// Rotate and pain black
					parent.color = NodeColor.Black;
					s.color = NodeColor.Black;
					sL.color = NodeColor.Black;
					rotateRight_nl(g, parent, s, sR);
				}
			}
			else if(pColor == NodeColor.Red)
			{
				return false;
				//rotateRight_nl(g, parent, s, sR);
			}
			else
			{
				// Classic case.
				s.color = NodeColor.Black;
				parent.color = NodeColor.Red;
				rotateRight_nl(g, parent, s, sR);
			}
		}

		//Assert.assertTrue(parent.parent == s);
		
		return true;
	}
	
	//TODO Recheck this!
	
	/**
	 * If n is red we can finish by painting it. Red + Black = Black
	 * If n is the root we can finish.
	 * n may be null.
	 * The while will only continue while we push black.
	 * @param node The node who is double black
	 */
	private void correctBlackCount(Node<K,V> node)
	{
		while(node != null && !isUnlinked(node.changeOVL))	
		{
			final Node<K,V> p = node.parent;
			final Node<K,V> g = p.parent;
			final char nDir = p.left == node ? Left : Right;
			
			Node<K,V> damaged = null;
			
			// We are root and can not be double black
			if(p == rootHolder) {
				synchronized (node) {
					if(!isUnlinked(node.changeOVL) && node.parent == rootHolder)
					{
						node.color = NodeColor.Black;
						return;
					}
					else {
						continue;
					}
				}
			}			
			
			synchronized (g) {
				if(p.parent != g || isUnlinked(g.changeOVL)) {
					continue;
				}
				synchronized (p) {
					if(node.parent != p || isUnlinked(p.changeOVL)) { 
						continue;
					}
					synchronized (node) {
						if(isUnlinked(node.changeOVL)) {
							continue;
						}

						if(node.color != NodeColor.DoubleBlack) {
							// Done someone fixed me up
							return;
						}
						
						// From here on we know node node is a double Black node
						final Node<K,V> s = p.sibling(nDir);
						
						//Assert.assertTrue(s != null);

						synchronized (s) {
							//Assert.assertTrue(s.parent == p);
							//Assert.assertTrue(!isUnlinked(s.changeOVL));

							final Node<K,V> sL = s.left;
							final Node<K,V> sR = s.right;

							final Node<K,V>[] result;
							if(s.color == NodeColor.Red) {
								result = resolveRedSibling(g, p, s, sL, sR, nDir);
							}
							else {
								result = resolveBlackNode(g, p, s, nDir);
							}
							
							if(result == null) { // Failure
								continue;
							}
							// Success
							node.color = NodeColor.Black; // If we got here it means we successfully removed it's extra black
							damaged = result[0];
							node = result[1];
						}// Synchronization s

					} // synchronization node
				} // synchronization parent
			} // Synchronization g
			
			if(damaged != null) {
				fixColorAndRebalance(damaged);
			}
		} // End of while
	}
	
	/**
	 * Rebalances sibling s with regards to double black node n. n must be locked on entry and at least one of sL and sR is Red
	 * @param g Locked
	 * @param parent Locked AND not red
	 * @param s locked AND is Black
	 * @param sL normalized AND Red if sR is Black
	 * @param sR normalized AND Red if sL is Black
	 * @param nDir direction of n. 
	 * @return a double black node if p is doubleBlack or null if not
	 */
	private Node<K,V> rebalanceSibling(final Node<K,V> g,final Node<K,V> parent,final Node<K,V> s, final Node<K,V> sL, final Node<K,V> sR, final char nDir)
	{
		//Assert.assertTrue(s.color == NodeColor.Black);
		//Assert.assertTrue(color(s.left) == NodeColor.Red || color(s.right) == NodeColor.Red);
		
		if(nDir == Left)
		{
			if(color(sR) == NodeColor.Red)
			{
				// If sR is red we can rotate left and not care about sL
				if(parent.color != NodeColor.DoubleBlack)
				{
					s.color = parent.color;
					sR.color = NodeColor.Black;
					parent.color = NodeColor.Black;
					rotateLeft_nl(g, parent, s, sL);
					return null;
				}
				else
				{
					s.color = NodeColor.DoubleBlack;
					sR.color = NodeColor.Black;
					parent.color = NodeColor.Black;
					rotateLeft_nl(g, parent, s, sL);
					return s;
				}
			}
			else
			{
				// sR is black so we have to do a double rotation
				synchronized (sL) {
					if(parent.color != NodeColor.DoubleBlack)
					{
			//			System.out.println(Thread.currentThread().getName() + " painted sL(" + sL.key + ") " + parent.color );
						sL.color = parent.color;
						parent.color = NodeColor.Black;
						//s.color = NodeColor.Black; Redundant
						rotateLeftOverRight_nl(g, parent, s, sL);
						return null;
					}
					else
					{
						sL.color = NodeColor.DoubleBlack;
						parent.color = NodeColor.Black;
						//s.color = NodeColor.Black; redundant
						rotateLeftOverRight_nl(g, parent, s, sL);
						return sL;
					}
				}
			} // sR.color = Black
		}// nDir == left
		else
		{			
			if(color(sL) == NodeColor.Red)
			{
				// If sL is red we can rotate right and not care about sL
				if(parent.color != NodeColor.DoubleBlack)
				{
					s.color = parent.color;
					sL.color = NodeColor.Black;
					parent.color = NodeColor.Black;
					rotateRight_nl(g, parent, s, sR);
					return null;
				}
				else
				{
					s.color = NodeColor.DoubleBlack;
					sL.color = NodeColor.Black;
					parent.color = NodeColor.Black;
					rotateRight_nl(g, parent, s, sR);
					return s;
				}
			}
			else
			{
				// sL is black so we have to do a double rotation
				synchronized (sR) {
					if(parent.color != NodeColor.DoubleBlack)
					{
						sR.color = parent.color;
						//s.color = NodeColor.Black; redundant
						parent.color = NodeColor.Black;
						rotateRightOverLeft_nl(g, parent, s, sR);
						return null;
					}
					else
					{
						//s.color = NodeColor.Black; Redundant
						sR.color = NodeColor.DoubleBlack;
						parent.color = NodeColor.Black;
						rotateRightOverLeft_nl(g, parent, s, sR);
						return sR;
					}
				}
			} // sL.color = Black
		}// nDir == Right
	}
	

	
	/**
	 * Remove a node with no sons (nL && nR == null)
	 * We have to have the locks of parent and n
	 * return nothing. The caller must saves n's old value
	 * @param parent
	 * @param n
	 * @return n.oldValue
	 */
	private void removeSonLessNode(final Node<K,V> parent, final Node<K,V> n)
	{
		//Assert.assertTrue(n.left == null);
		//Assert.assertTrue(n.right == null);
		//Assert.assertTrue(Thread.holdsLock(parent));
		//Assert.assertTrue(Thread.holdsLock(n));
		//Assert.assertTrue(n.color != NodeColor.DoubleBlack);
		
		if(n == parent.left) {
			parent.left = null;
		}
		else {
			parent.right = null;
		}
		
		n.changeOVL = UnlinkedOVL;
		n.value = null;
	}
	

	
	/**
	 * p.left -> n.right -> nR
	 * Rotates first Left and then right
	 * @param g
	 * @param p
	 * @param n
	 * @param nR
	 * @return
	 */
	private void rotateRightOverLeft_nl(
			final Node<K,V> g,
			final Node<K,V> p,
			final Node<K,V> n,
			final Node<K,V> nR) {
		
        final long pOVL = p.changeOVL;
        final long nOVL = n.changeOVL;
        final long nROVL = nR.changeOVL;

        final Node<K,V> gL = g.left;
        final Node<K,V> nRL = nR.left;
        final Node<K,V> nRR = nR.right;

        p.changeOVL = beginShrink(pOVL);
        n.changeOVL = beginShrink(nOVL);
        nR.changeOVL = beginGrow(nROVL);

        p.left = nRR;
        n.right = nRL;
        nR.left = n;
        nR.right = p;
        if (gL == p) {
            g.left = nR;
        } else {
            g.right = nR;
        }

        nR.parent = g;
        n.parent = nR;
        p.parent = nR;
        if (nRR != null) {
            nRR.parent = p;
        }
        if (nRL != null) {
            nRL.parent = n;
        }
        
        nR.changeOVL = endGrow(nROVL);
        n.changeOVL = endShrink(nOVL);
        p.changeOVL = endShrink(pOVL);
	}

	/**
	 * p.right -> n.left -> nL
	 * Rotates first Right and then Left
	 * @param g
	 * @param p
	 * @param n
	 * @param nL
	 * @return
	 */
	private void rotateLeftOverRight_nl(
			final Node<K,V> g,
			final Node<K,V> p,
			final Node<K,V> n,
			final Node<K,V> nL) {
		
        final long pOVL = p.changeOVL;
        final long nOVL = n.changeOVL;
        final long nLOVL = nL.changeOVL;

        final Node<K,V> gL = g.left;
        final Node<K,V> nLR = nL.right;
        final Node<K,V> nLL = nL.left;

        p.changeOVL = beginShrink(pOVL);
        n.changeOVL = beginShrink(nOVL);
        nL.changeOVL = beginGrow(nLOVL);

        p.right = nLL;
        n.left = nLR;
        nL.left = p;
        nL.right = n;
        if(gL == p) {
        	g.left = nL;
        }
        else {
        	g.right = nL;
        }
      
        nL.parent = g;
        n.parent = nL;
        p.parent = nL;
        if(nLL != null) {
        	nLL.parent = p;
        }
        if(nLR != null) {
        	nLR.parent = n;
        }
        
        nL.changeOVL = endGrow(nLOVL);
        n.changeOVL = endShrink(nOVL);
        p.changeOVL = endShrink(pOVL);
	}
	
	private void rotateRight_nl(
			final Node<K,V> g,
			final Node<K,V> p,
			final Node<K,V> n,
			final Node<K,V> nR) {

		final long pOVL = p.changeOVL;
		final long nOVL = n.changeOVL;
		
		final Node<K,V> gL = g.left; // Used to determine if p is left or right son of g.
		
		n.changeOVL = beginGrow(nOVL);
		p.changeOVL = beginShrink(pOVL);
		
        // Down links originally to shrinking nodes should be the last to change,
        // because if we change them early a search might bypass the OVL that
        // indicates its invalidity.  Down links originally from shrinking nodes
        // should be the first to change, because we have complete freedom when to
        // change them.
		
		p.left = nR;
		n.right = p;
		if(gL == p)
		{
			g.left = n;
		}
		else
		{
			g.right = n;
		}
		
		n.parent = g;
		p.parent = n;
		if(nR != null) {
			nR.parent = p;
		}
		
		n.changeOVL = endGrow(nOVL);
		p.changeOVL  = endShrink(pOVL);
	}
	
	private void rotateLeft_nl(
			final Node<K,V> g,
			final Node<K,V> p,
			final Node<K,V> n,
			final Node<K,V> nL) {

		final long pOVL = p.changeOVL;
		final long nOVL = n.changeOVL;
		
		final Node<K,V> gL = g.left;
		
		n.changeOVL = beginGrow(nOVL);
		p.changeOVL = beginShrink(pOVL);
		
        // Down links originally to shrinking nodes should be the last to change,
        // because if we change them early a search might bypass the OVL that
        // indicates its invalidity.  Down links originally from shrinking nodes
        // should be the first to change, because we have complete freedom when to
        // change them.
		
		p.right = nL;
		n.left = p;
		if(gL == p) {
			g.left = n;
		}
		else {
			g.right = n;
		}
		
		n.parent = g;
		p.parent = n;
		if(nL != null) {
			nL.parent = p;
		}

		n.changeOVL = endGrow(nOVL);
		p.changeOVL  = endShrink(pOVL);
	}
	
// Test Functions --------------------------------------------------------------------------------------------------------------------------------------------------------------	
	
//	public void printCompleteTreeKey()
//	{	
//		System.out.println("#################################");
//		if(rootHolder.left != null)
//			rootHolder.left.printNodeKey(0,-1);
//	}
//	
//	public void printCompleteTreeKey(int id)
//	{
//		System.out.println("################################# " + id);
//		if(rootHolder.left != null)
//			rootHolder.left.printNodeKey(0,id);
//	}
	
//	public boolean checkOrder()
//	{
//		if(this.isEmpty())
//			return true;
//		
//		return checkOrder(rootHolder.left);
//	}
//	
//	private boolean checkOrder(Node<K,V> n) {
//		final Comparable<? super K> k = comparable(n.key);
//		
//		if(n.left != null)
//		{
//			if(k.compareTo(n.left.key) < 0)
//				return false;
//			if(!checkOrder(n.left))
//				return false;
//		}
//		if(n.right != null)
//		{
//			if(k.compareTo(n.right.key) > 0)
//				return false;
//			if(!checkOrder(n.right))
//				return false;
//		}
//		return true;
//	}
	
//	// Checks balance of tree. Checks all properties
//	public boolean checkBalance()
//	{
//		// Check root
//		if(rootHolder.left == null)
//			return true;
//		if(rootHolder.left.color == NodeColor.Red)
//		{
//			System.out.println("Error at root. Root was red");
//			return false;
//		}
//		
//		return compareBalance(rootHolder.left);
//	}
//	
//	private boolean compareBalance(Node<K,V> n)
//	{
//		//Check red
//		//(The root can't be red so this does not fail at root)
//		if(n.color == NodeColor.Red && n.parent.color == NodeColor.Red)
//			return foundError(n,"Red red");
//		
//		int amBlack = 0;
//		if(n.color == NodeColor.Black)
//			amBlack = 1;
//		
//		if(n.left != null)
//		{
//			if(!compareBalance(n.left))
//				return foundError(n);
//		}
//		
//		if(n.right != null)
//		{
//			if(!compareBalance(n.right))
//				return foundError(n);
//		}
//		
//		if(n.left == null && n.right == null)
//		{
//			n.blackHeight = 1 + amBlack;
//			return true;
//		}
//		
//			
//		if(n.left != null && n.right != null && n.left.blackHeight != n.right.blackHeight)
//			return foundError(n,"Black height");
//		
//		if(n.left == null && n.right.blackHeight != 1)
//			return foundError(n,"Null with black");
//		
//		if(n.right == null && n.left.blackHeight != 1)
//			return foundError(n, "Null with black");
//		
//		if(n.left == null)
//		{
//			n.blackHeight = 1 + amBlack;
//		}
//		else
//		{
//			n.blackHeight = n.left.blackHeight + amBlack;
//		}
//		
//		return true;
//	}
//	
//	private boolean foundError(Node<K,V> n, String message)
//	{
//		System.out.println(n.key + "\t" + message);
//		return false;
//	}
//	
//	private boolean foundError(Node<K,V> n)
//	{
//		System.out.println(n.key);
//		return false;
//	}
	
//	public AtomicInteger wasTadaad = new AtomicInteger(0);
//	public AtomicInteger blackProblem = new AtomicInteger(0);
//	public AtomicInteger redProblem = new AtomicInteger(0);
//	
//	public AtomicInteger yields = new AtomicInteger(0);
//	public AtomicInteger blackYields = new AtomicInteger(0);
//	public AtomicInteger redYields = new AtomicInteger(0);
//	public AtomicInteger waits = new AtomicInteger(0);
//	public AtomicInteger blackWaits = new AtomicInteger(0);
//	public AtomicInteger redWaits = new AtomicInteger(0);


	
//	public int countUnlinkedNodes()
//	{
//		return unlinkedCount(rootHolder.left);
//	}
//
//	private int unlinkedCount(Node<K,V> n) {
//		if(n == null)
//			return 0;
//		
//		if(n.value ==  null)
//			return unlinkedCount(n.left) + unlinkedCount(n.right)+1;
//		else
//			return unlinkedCount(n.left) + unlinkedCount(n.right);
//	}
	//Tests!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	

	
//	public void generateTree(int[] numbers, boolean[] colors, int[] nulls)
//	{
//		System.out.println(numbers.length + "\tColors " + colors.length );
//		
//		Node<Integer,Integer>[] nodes = new Node<Integer,Integer>[numbers.length];
//		rootHolder.left = new Node<Integer,Integer>(numbers[0],numbers[0],rootHolder,0L,Black,null,null);
//		nodes[0] = rootHolder.left;
//		int j = 0;
//		if(nulls[0] == numbers[0])
//		{
//			rootHolder.left.value = null;
//			j++;
//		}
//				
//		for(int i = 1; i < numbers.length-1; i=i+2)
//		{
//			Node<K,V> currentNode = nodes[(i+1)/2 - 1];
//			
//			if(numbers[i] != -1)
//			{
//				if(j >= nulls.length || nulls[j] != numbers[i])
//					currentNode.left = new Node<K,V>(numbers[i],numbers[i],currentNode,0L,colors[i],null,null);
//				else {
//					currentNode.left = new Node<K,V>(numbers[i],null,currentNode,0L,colors[i],null,null);
//					j++;
//				}
//				nodes[i] = currentNode.left;
//			}
//			
//			if(numbers[i+1] != -1)
//			{
//				if(j >= nulls.length || nulls[j] != numbers[i+1])
//					currentNode.right = new Node<K,V>(numbers[i+1],numbers[i+1],currentNode,0L,colors[i+1],null,null);
//				else {
//					currentNode.right = new Node<K,V>(numbers[i+1],null,currentNode,0L,colors[i+1],null,null);
//					j++;
//				}
//				nodes[i+1] = currentNode.right;
//			}
//		}
//	}
//	
//	public void LockNode(int who) {
//		
//		Node<K,V> currentNode = rootHolder.left;
//		 while(true)
//		 {
//			 if(currentNode == null)
//				 break;
//			 
//			 if(currentNode.key == who) {
//				 synchronized (currentNode) {
//					synchronized (this) {
//						try {
//							this.wait();
//						} catch (InterruptedException e) {
//							break;
//						}
//					}
//				}
//			 }
//			 else
//			 {
//				 if(currentNode.key < who)
//					 currentNode = currentNode.right;
//				 else
//					 currentNode = currentNode.left;
//			 }
//		 }
//	}
	
	   @Override
	    public Set<Map.Entry<K,V>> entrySet() {
	        return null;
	    }

	    private class EntrySet extends AbstractSet<Entry<K,V>> {

	        @Override
	        public int size() {
	            return 0;
	        }

	        @Override
	        public boolean isEmpty() {
	            return false;
	        }

	        @Override
	        public void clear() {
	        }

	        @Override
	        public boolean contains(final Object o) {
	            return false;
	        }



	        @Override
	        public boolean remove(final Object o) {
	            if (!(o instanceof Entry<?,?>)) {
	                return false;
	            }
	            final Object k = ((Entry<?,?>)o).getKey();
	            final Object v = ((Entry<?,?>)o).getValue();
	            return false;
	        }

			@Override
			public Iterator<Entry<K, V>> iterator() {
				// TODO Auto-generated method stub
				return null;
			}

	    }

	    private class EntryIter implements Iterator<Entry<K,V>> {

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Entry<K, V> next() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				
			}

	    }
	
}
