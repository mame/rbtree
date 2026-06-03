package com.headius.jruby;

import SimpleConcurrent.ConcurrentRBTree;
import org.jruby.ObjectFlags;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyEnumerator;
import org.jruby.RubyFixnum;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyProc;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.Helpers;
import org.jruby.runtime.Signature;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.util.TypeConverter;

import java.io.IOException;
import java.util.Map;

public class RBTreeLibrary implements Library {
    @Override
    public void load(Ruby runtime, boolean b) throws IOException {
        MultiRBTreeClass = runtime.defineClass("MultiRBTree", runtime.getObject(), MultiRBTree::new);
        RBTreeClass = runtime.defineClass("RBTree", MultiRBTreeClass, RBTree::new);

        MultiRBTreeClass.defineAnnotatedMethods(MultiRBTree.class);

        id_cmp = runtime.newSymbol("<=>");
        id_call = runtime.newSymbol("call");
        id_default = runtime.newSymbol("default");
        id_flatten_bang = runtime.newSymbol("flatten!");
        id_breakable = runtime.newSymbol("breakable");
        id_comma_breakable = runtime.newSymbol("comma_breakable");
        id_group = runtime.newSymbol("group");
        id_object_group = runtime.newSymbol("object_group");
        id_pp = runtime.newSymbol("pp");
        id_text = runtime.newSymbol("text");
    }

    RubyClass MultiRBTreeClass;
    RubyClass RBTreeClass;

    private RubySymbol id_cmp;
    private RubySymbol id_call;
    private RubySymbol id_default;
    private RubySymbol id_flatten_bang;
    private RubySymbol id_breakable;
    private RubySymbol id_comma_breakable;
    private RubySymbol id_group;
    private RubySymbol id_object_group;
    private RubySymbol id_pp;
    private RubySymbol id_text;

    @JRubyMethod(meta = true)
    public static IRubyObject _load(ThreadContext context, IRubyObject self, IRubyObject arg0) {

    }

    @JRubyMethod(name = "[]", meta = true)
    public static IRubyObject op_aref(ThreadContext context, IRubyObject self, IRubyObject arg0) {

    }

    private static IRubyObject enumSize(ThreadContext context, MultiRBTree tree, IRubyObject[] args) {
        return RubyFixnum.newFixnum(context.runtime, tree.rbTree.size());
    }

    int RBTREE_PROC_DEFAULT = ObjectFlags.registry.newFlag(MultiRBTree.class);

    public class MultiRBTree extends RubyObject {
        private final ConcurrentRBTree<IRubyObject, IRubyObject> rbTree;
        private volatile int iterLevel;
        private IRubyObject ifnone;

        public MultiRBTree(Ruby runtime, RubyClass klass) {
            super(runtime, klass);

            rbTree = new ConcurrentRBTree<>();
        }

        @JRubyMethod
        public IRubyObject initialize(ThreadContext context, IRubyObject[] args, Block block) {
            rbtree_modify(context);

            if (block.isGiven()) {
                RubyProc proc;

                Arity.checkArgumentCount(context, args, 0, 0);
                proc = RubyProc.newProc(context.runtime, block, block.type);
                rbtree_check_proc_arity(context, proc, 2);
                ifnone = proc;
                setFlag(RBTREE_PROC_DEFAULT, true);
            } else {
                rbtree_check_argument_count(ThreadContext context, args.length, 0, 1);
                if (args.length == 1) {
                    ifnone = args[0];
                }
            }

            return this;
        }

        private void rbtree_modify(ThreadContext context) {
            if (iterLevel > 0) {
                throw context.runtime.newTypeError("can't modify rbtree during iteration");
            }

            checkFrozen();
        }

        private void rbtree_check_proc_arity(ThreadContext context, RubyProc proc, const int expected) {
            if (proc.getBlock().type == Block.Type.LAMBDA) {
                Signature signature = proc.getBlock().getSignature();
                int arity = signature.arityValue();
                int min = arity < 0 ? -arity - 1 : arity;
                int max = arity < 0 ? Integer.MAX_VALUE : arity;
                if (expected < min || expected > max) {
                    throw context.runtime.newTypeError("proc takes " + expected + " arguments");
                }
            }
        }

        private void rbtree_check_argument_count(ThreadContext context, int argc, int min, int max) {
            if (argc < min || argc > max) {
                String message = "wrong number of arguments";
                if (min == max) {
                    throw context.runtime.newArgumentError(String.format("%s (%d for %d)", message, argc, min));
                } else if (max == Integer.MAX_VALUE) {
                    throw context.runtime.newArgumentError(String.format("%s (%d for %d+)", message, argc, -min - 1));
                } else {
                    throw context.runtime.newArgumentError(String.format("%s (%d for %d..%d)", message, argc, min, max));
                }
            }
        }

        @JRubyMethod
        public IRubyObject initialize_copy(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public RubyArray<IRubyObject> to_a(ThreadContext context) {
            RubyArray<IRubyObject> ary = RubyArray.newArray(context.runtime, rbTree.size());
            rbTree.forEach((key, value) -> ary.append(RubyArray.newArray(context.runtime, key, value)));
            return ary;
        }

        @JRubyMethod(name = {"to_h", "to_hash"})
        public IRubyObject to_h(ThreadContext context) {
            if (!(this instanceof RBTree)) {
                throw context.runtime.newTypeError("can't convert MultiRBTree to Hash");
            }

            RubyHash hash = RubyHash.newHash(context.runtime);
            rbTree.forEach((key, value) -> hash.op_aset(context, key, value));
            hash.default_value_set(context, ifnone);
            if (getFlag(RBTREE_PROC_DEFAULT)) {
                hash.setFlag(ObjectFlags.PROCDEFAULT_HASH_F, true);
            }

            return hash;
        }

        @JRubyMethod
        public IRubyObject to_rbtree(ThreadContext context) {
            return this;
        }

        @JRubyMethod(alias = "to_s")
        public IRubyObject inspect(ThreadContext context) {
        }

        @JRubyMethod(name = "==")
        public IRubyObject op_equal(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "[]")
        public IRubyObject op_aref(ThreadContext context, IRubyObject key) {
            IRubyObject value = rbTree.get(key);

            if (value == null) {
                return callMethod(context, "default", key);
            }

            return value;
        }

        @JRubyMethod
        public IRubyObject fetch(ThreadContext context, IRubyObject key, Block block) {
            IRubyObject value = rbTree.get(key);
            if (value != null) {
                return value;
            }

            if (block.isGiven()) {
                return block.yieldSpecific(context, key);
            }

            throw context.runtime.newIndexError("key not found");
        }

        @JRubyMethod
        public IRubyObject fetch(ThreadContext context, IRubyObject key, IRubyObject def, Block block) {
            if (block.isGiven()) {
                context.runtime.getWarnings().warn("block supersedes default value argument");
            }

            IRubyObject value = rbTree.get(key);
            if (value != null) {
                return value;
            }

            if (block.isGiven()) {
                return block.yieldSpecific(context, key);
            }
            return def;
        }

        @JRubyMethod
        public IRubyObject lower_bound(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject upper_bound(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject bound(ThreadContext context, IRubyObject[] args) {
        }

        @JRubyMethod
        public IRubyObject first(ThreadContext context) {
        }

        @JRubyMethod
        public IRubyObject last(ThreadContext context) {
        }

        @JRubyMethod(name = {"[]=", "store"})
        public IRubyObject op_aset(ThreadContext context, IRubyObject key, IRubyObject value) {
            rbtree_modify(context);

            rbTree.put(key, value);

            return value;
        }

        @JRubyMethod(name = "default")
        public IRubyObject default_get(ThreadContext context) {
            if (getFlag(RBTREE_PROC_DEFAULT)) {
                return context.nil;
            }
            return ifnone;
        }

        @JRubyMethod(name = "default")
        public IRubyObject default_get(ThreadContext context, IRubyObject def) {
            if (getFlag(RBTREE_PROC_DEFAULT)) {
                return Helpers.invoke(context, ifnone, "call", this, def);
            }
            return ifnone;
        }

        @JRubyMethod(name = "default=")
        public IRubyObject default_set(ThreadContext context, IRubyObject ifnone) {
            rbtree_modify(context);
            this.ifnone = ifnone;
            setFlag(RBTREE_PROC_DEFAULT, false);
            return ifnone;
        }

        @JRubyMethod
        public IRubyObject default_proc(ThreadContext context) {
            if (getFlag(RBTREE_PROC_DEFAULT)) {
                return ifnone;
            }
            return context.nil;
        }

        @JRubyMethod(name = "default_proc=")
        public IRubyObject default_proc_set(ThreadContext context, IRubyObject proc) {
            IRubyObject temp;

            rbtree_modify(context);
            if (proc.isNil()) {
                ifnone = context.nil;
                setFlag(RBTREE_PROC_DEFAULT, false);
                return context.nil;
            }

            temp = TypeConverter.convertToType(context, proc, context.runtime.getProc(), "to_proc", false);
            if (temp.isNil()) {
                throw context.runtime.newTypeError("wrong default_proc type " + proc.getMetaClass().getName() + " (expected Proc)");
            }
            rbtree_check_proc_arity(context, (RubyProc) temp, 2);
            ifnone = temp;
            setFlag(RBTREE_PROC_DEFAULT, true);
            return proc;
        }

        @JRubyMethod
        public IRubyObject key(ThreadContext context, IRubyObject key) {
            return RubyBoolean.newBoolean(context, rbTree.containsKey(key));
        }

        @JRubyMethod
        public IRubyObject index(ThreadContext context, IRubyObject key) {
            context.runtime.getWarnings().warnDeprecatedAlternate("index", "key");
            return key(context, key);
        }

        @JRubyMethod(name = "empty?")
        public IRubyObject empty_p(ThreadContext context) {
            return RubyBoolean.newBoolean(context, rbTree.isEmpty());
        }

        @JRubyMethod(name = {"size", "length"})
        public IRubyObject size(ThreadContext context) {
            return RubyFixnum.newFixnum(context.runtime, rbTree.size());
        }

        @JRubyMethod(name = {"each", "each_pair"})
        public IRubyObject each(ThreadContext context, Block block) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, "each", RBTreeLibrary::enumSize);
            }

            rbTree.forEach((key, value) -> block.yieldSpecific(context, key, value));

            return this;
        }

        @JRubyMethod
        public IRubyObject each_value(ThreadContext context, Block block) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, "each_value", RBTreeLibrary::enumSize);
            }

            rbTree.forEach((key, value) -> block.yieldSpecific(context, value));

            return this;
        }

        @JRubyMethod
        public IRubyObject each_key(ThreadContext context, Block block) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, "each_key", RBTreeLibrary::enumSize);
            }

            rbTree.forEach((key, value) -> block.yieldSpecific(context, key));

            return this;
        }

        @JRubyMethod
        public IRubyObject reverse_each(ThreadContext context, Block block) {
            // TODO: support for reverse forEach in ConcurrentRBTree
            return each(context, block);
        }

        @JRubyMethod
        public IRubyObject keys(ThreadContext context) {
            return RubyArray.newArray(context.runtime, rbTree.keySet());
        }

        @JRubyMethod
        public IRubyObject values(ThreadContext context) {
            return RubyArray.newArray(context.runtime, rbTree.values());
        }

        @JRubyMethod
        public IRubyObject values_at(ThreadContext context, IRubyObject[] args) {
            RubyArray ary = RubyArray.newArray(context.runtime, args.length);

            for (int i = 0; i < args.length; i++) {
                ary.push(op_aref(context, args[i]));
            }
            return ary;
        }

        @JRubyMethod
        public IRubyObject shift(ThreadContext context) {
        }

        @JRubyMethod
        public IRubyObject pop(ThreadContext context) {
        }

        @JRubyMethod
        public IRubyObject delete(ThreadContext context, IRubyObject key, Block block) {
            IRubyObject value;

            rbtree_modify(context);
            value = rbTree.remove(key);
            if (value == null) {
                return block.isGiven() ? block.yieldSpecific(context, key) : context.nil;
            }
            return value;
        }

        @JRubyMethod
        public IRubyObject delete_if(ThreadContext context, Block block) {
            return rbtree_remove_if(context, block, "delete_if", true);
        }

        @JRubyMethod
        public IRubyObject keep_if(ThreadContext context, Block block) {
            return rbtree_remove_if(context, block, "keep_if", false);
        }

        private IRubyObject rbtree_remove_if(ThreadContext context, Block block, String method, boolean if_true) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, method, RBTreeLibrary::enumSize);
            }
            rbtree_modify(context);

            for (Map.Entry<IRubyObject, IRubyObject> entry : rbTree.entrySet()) {
                IRubyObject key = entry.getKey();
                IRubyObject value = entry.getValue();
                if (block.yieldSpecific(context, key, value).isTrue() == isTrue()) rbTree.remove(key, value);
            }

            return this;
        }

        @JRubyMethod
        public IRubyObject reject(ThreadContext context, Block block) {
            return rbtree_select_if(context, block, "reject", false);
        }

        @JRubyMethod(name = "reject!")
        public IRubyObject reject_bang(ThreadContext context, Block block) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, "reject!", RBTreeLibrary::enumSize);
            }
            int size = rbTree.size();
            delete_if(context, block);
            if (size == rbTree.size()) {
                return context.nil;
            }
            return this;
        }

        @JRubyMethod
        public IRubyObject select(ThreadContext context, Block block) {
            return rbtree_select_if(context, block, "reject", true);
        }

        private IRubyObject rbtree_select_if(ThreadContext context, Block block, String method, boolean if_true) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, method, RBTreeLibrary::enumSize);
            }
            MultiRBTree newTree = (MultiRBTree) allocateNew(context);
            rbTree.forEach((key, value) -> { if (block.yieldSpecific(context, key, value).isTrue() == if_true) newTree.rbTree.put(key, value)});
            return newTree;
        }

        protected MultiRBTree allocateNew(ThreadContext context) {
            return new MultiRBTree(context.runtime, MultiRBTreeClass);
        }

        @JRubyMethod(name = "select!")
        public IRubyObject select_bang(ThreadContext context, Block block) {
            if (!block.isGiven()) {
                return RubyEnumerator.enumeratorizeWithSize(context, this, "select!", RBTreeLibrary::enumSize);
            }
            int size = rbTree.size();
            keep_if(context, block);
            if (size == rbTree.size()) {
                return context.nil;
            }
            return this;
        }

        @JRubyMethod
        public IRubyObject clear(ThreadContext context) {
            rbtree_modify(context);
            rbTree.clear();
            return this;
        }

        @JRubyMethod
        public IRubyObject invert(ThreadContext context) {
            MultiRBTree newTree = allocateNew(context);
            rbTree.forEach((key, value) -> newTree.rbTree.put(value, key));
            return newTree;
        }

        @JRubyMethod
        public IRubyObject update(ThreadContext context, IRubyObject other, Block block) {
            rbtree_modify(context);

            if (this == other) {
                return this;
            }
            if (!other.getMetaClass().isKindOfModule(getMetaClass())) {
                throw context.runtime.newTypeError("wrong argument type " + other.getType().getName() + " (expected " + getType().getName() + ")");
            }
            MultiRBTree otherTree = (MultiRBTree) other;

            if (block.isGiven()) {
                otherTree.rbTree.forEach((key, value) -> {
                    rbTree.compute(key, (k, v) -> {
                        return k == null ? v : block.yieldSpecific(context, k, op_aref(context, k), v);
                    });
                });
            } else {
                otherTree.rbTree.forEach(rbTree::replace);
            }
            return this;
        }

        @JRubyMethod(name = "merge!")
        public IRubyObject merge_bang(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject merge(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject replace(ThreadContext context, IRubyObject arg0) {
        }
#

        @JRubyMethod
        public IRubyObject flatten(ThreadContext context, IRubyObject[] args) {
        }

        @JRubyMethod(name = "include?")
        public IRubyObject include_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "member?")
        public IRubyObject member_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "has_key?")
        public IRubyObject has_key_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "has_value?")
        public IRubyObject has_value_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "key?")
        public IRubyObject key_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod(name = "value?")
        public IRubyObject value_p(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject readjust(ThreadContext context, IRubyObject[] args) {
        }

        @JRubyMethod
        public IRubyObject cmp_proc(ThreadContext context) {
        }

        @JRubyMethod
        public IRubyObject _dump(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject pretty_print(ThreadContext context, IRubyObject arg0) {
        }

        @JRubyMethod
        public IRubyObject pretty_print_cycle(ThreadContext context, IRubyObject arg0) {

        }
    }

    public class RBTree extends MultiRBTree {
        public RBTree(Ruby runtime, RubyClass klass) {
            super(runtime, klass);
        }

        protected MultiRBTree allocateNew(ThreadContext context) {
            return new RBTree(context.runtime, RBTreeClass);
        }
    }
}
