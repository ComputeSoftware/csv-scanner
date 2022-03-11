(ns computesoftware.csv-scanner)

;; This CSV scanner assumes an ASCII-compatible encoding (including UTF-8)
;; It's fully stateful.
;; A scanner is created from an InputStream.
;; You first call .seek on it, the numbers of read fields is returned or -1 on EOF
;; Look at the field defn for an example of how to retrieve data for a specific field.
;; WARNING; arrays returned by .bytes and .delims are reused so all data of interest
;; should be read before the next call to .seek.


(definterface ICsvRow
  (^int fieldsCount [])
  (^bytes bytes [])
  (^int shift [])
  (^ints delims []))

(definterface ICsvKey
  (^computesoftware.csv_scanner.ICsvRow row [])
  (^ints runs []))

(definterface ICsvScanner
  (^int seek [])
  (headers [])
  (headers [headers-map])
  (^computesoftware.csv_scanner.ICsvRow row [])
  (^computesoftware.csv_scanner.ICsvKey key [^ints runs])
  (^java.nio.ByteBuffer bb []))

(defn field
  [^ICsvRow row ^long i ^java.nio.charset.Charset charset]
  (let [ds (.delims row)
        from (aget ds i)
        to (unchecked-dec-int (aget ds (inc i)))
        s (String. (.bytes row) (unchecked-subtract-int from (.shift row)) (- to from) charset)]
    (if (.startsWith s "\"")
      (.replace (subs s 1 (dec (.length s))) "\"\"" "\"")
      s)))

(deftype CsvRow [^bytes bs ^ints delims ^java.nio.charset.Charset charset headers-map]
  ICsvRow
  (fieldsCount [_] (unchecked-dec-int (alength delims)))
  (bytes [_] bs)
  (shift [_] (aget delims 0))
  (delims [_] delims)
  clojure.lang.IPersistentCollection
  (seq [row]
    (seq (map #(field row % charset) (range 0 (unchecked-dec-int (alength delims))))))
  (count [_] (unchecked-dec-int (alength delims)))
  (equiv [_ o]
    (and (instance? CsvRow o)
      (java.util.Arrays/equals ^bytes (.bytes ^CsvRow o) ^bytes bytes)))
  clojure.lang.ILookup
  (valAt [row k] (.valAt row k nil))
  (valAt [row k not-found]
    (if-some [i (get headers-map k)]
      (if (< -1 i (count row))
        (field row i charset)
        not-found)))
  clojure.lang.Indexed
  (nth [row n] (.nth row n nil))
  (nth [row n not-found]
    (if (< -1 n (count row))
      (field row n charset)
      not-found)))
#_
        (defn cols-eq-pred
          "Takes a collection of column indices and returns a predicate for fast equality check of rows on this columns."
          [col-idxs]
          (let [runs (int-array (cols-runs col-idxs))
                n (alength runs)]
            (fn [^ICsvRow r1 ^ICsvRow r2]
              (let [a1 (.bytes r1)
                    o1 (.delims r1)
                    a2 (.bytes r2)
                    o2 (.delims r2)]
                (loop [i 0]
                  (if (< i n)
                    (let [from (aget runs i)
                          i (unchecked-inc i)
                          to (aget runs i)
                          i (unchecked-inc i)
                          from1 (unchecked-inc (aget o1 from))
                          to1 (aget o1 to)
                          from2 (unchecked-inc (aget o2 from))
                          to2 (aget o2 to)]
                      (if (java.util.Arrays/equals a1 from1 to1 a2 from2 to2)
                        (recur i)
                        false))
                    true))))))

(defn key-hash [^java.nio.ByteBuffer bb ^ints os ^ints runs]
  (let [n (alength runs)]
    (loop [i 0 from 0 to-8 -8 h (int 1)]
      #_(prn i from to-8 h n)
      (if (<= from to-8)
        (let [x (.getLong bb from)]
          (recur i (unchecked-add-int from 8) to-8 (unchecked-add-int (unchecked-multiply-int 31 h)
                                                     (unchecked-int (bit-xor x (unsigned-bit-shift-right x 32))))))
        (let [trail (bit-and 7 (unchecked-subtract from to-8))
              x (if (pos? trail) (unsigned-bit-shift-right (.getLong bb from) (bit-shift-left trail 3)) 0)
              h (unchecked-add-int (unchecked-multiply-int 31 h)
                  (unchecked-int (bit-xor x (unsigned-bit-shift-right x 32))))]
          #_(prn 'TRAIL trail 'X x)
          (if (< i n)
            (let [from (unchecked-inc (aget os (aget runs i)))
                  i (unchecked-inc i)
                  to-8 (unchecked-subtract (aget os (aget runs i)) 8)
                  i (unchecked-inc i)]
              (recur i from to-8 h))
            h))))))

(deftype CsvKey [^CsvRow _row ^ints _runs ^int h]
  ICsvKey
  (runs [_] _runs)
  (row [_] _row)
  clojure.lang.IDeref
  (deref [self] self)
  Object
  (hashCode [_] h)
  (equals [_ other]
    (and (instance? ICsvKey other)
      (let [n (alength _runs)
            a1 (.bytes _row)
            o1 (.delims _row)
            s1 (.shift _row)
            other-row (.row ^ICsvKey other)
            ^ints runs2 (.runs ^ICsvKey other)
            a2 (.bytes other-row)
            o2 (.delims other-row)
            s2 (.shift other-row)]
        (and (= n (alength runs2))
          (loop [i 0]
            (if (< i n)
              (let [from1 (aget _runs i)
                    from2 (aget runs2 i)
                    i (unchecked-inc i)
                    to1 (aget _runs i)
                    to2 (aget runs2 i)
                    i (unchecked-inc i)
                    from1 (unchecked-subtract-int (aget o1 from1) s1)
                    to1 (unchecked-subtract-int (unchecked-dec-int (aget o1 to1)) s1)
                    from2 (unchecked-subtract-int (aget o2 from2) s2)
                    to2 (unchecked-subtract-int (unchecked-dec-int (aget o2 to2)) s2)]
                (if (java.util.Arrays/equals a1 from1 to1 a2 from2 to2)
                  (recur i)
                  false))
              true))))))
  (toString [_]
    (pr-str
      (for [[from to] (partition 2 _runs)
            i (range from to)]
        (nth _row i)))))

(declare ->ScannerKey)

(defmacro ^:private aensure [a n]
  `(if (= ~n (alength ~a))
     (set! ~a (java.util.Arrays/copyOf ~a (* 2 ~n)))
     ~a))

(defmacro ^:private bbensure [bb n]
  `(if (= ~n (.capacity ~bb))
     (set! ~bb (java.nio.ByteBuffer/wrap (java.util.Arrays/copyOf (.array ~bb) (* 2 ~n))))
     ~bb))

(defmacro ^:private UL [s] (Long/parseUnsignedLong s 16))

(defmacro ^:private MASK [c] (* 0x0101010101010101 (bit-xor 0x7F (long c))))

(deftype Scanner [^:unsynchronized-mutable ^java.nio.ByteBuffer bbuf
                  ^:unsynchronized-mutable ^long rem-flags
                  ^:unsynchronized-mutable ^long rem-idx
                  ^:unsynchronized-mutable ^long blength
                  ^:unsynchronized-mutable ^ints offsets
                  ^:unsynchronized-mutable ^long olength
                  ^java.io.InputStream in
                  ^java.nio.charset.Charset charset
                  ^:unsynchronized-mutable headers-map]
  java.io.Closeable
  (close [_] (.close in))
  clojure.lang.Counted
  (count [_] olength)
  clojure.lang.ILookup
  (valAt [row k] (.valAt row k nil))
  (valAt [row k not-found]
    (if-some [i (get headers-map k)]
      (if (< -1 i (count row))
        (field row i charset)
        not-found)))
  clojure.lang.Indexed
  (nth [row n] (.nth row n nil))
  (nth [row n not-found]
    (if (< -1 n (count row))
      (field row n charset)
      not-found))
  ICsvRow
  (fieldsCount [_] olength)
  (bytes [_] (.array bbuf))
  (shift [_] -1)
  (delims [_] offsets)
  ICsvScanner
  (bb [_] bbuf)
  (headers [_] headers-map)
  (headers [_ headers] (set! headers-map headers))
  (row [_]
    (let [delims (java.util.Arrays/copyOf offsets (unchecked-inc-int olength))
          from (unchecked-inc-int (aget delims 0))
          to (aget delims olength)]
      (CsvRow. (java.util.Arrays/copyOfRange (.array bbuf) from to) delims charset headers-map)))
  (key [s runs]
    (->ScannerKey s runs))
  (seek [_]
    (aset offsets 0 (aget offsets olength))
    (loop [quoted false idx rem-idx flags rem-flags oi 1]
      (if (zero? flags)
        (let [idx+8 (unchecked-add idx 8)]
          (if (< idx+8 (bit-and -8 blength)) ; blength - (blength mod 8)
            ; enough data
            (let [n (.getLong bbuf idx+8)]
              (if (= n 0x2c2c2c2c2c2c2c2c) ; 8 commas
                (if quoted
                  (recur quoted idx+8 0 oi)
                  (let [oi' (unchecked-add oi 8)
                        oa (aensure offsets oi')]
                    (loop [oi oi idx idx+8]
                      (aset oa oi idx)
                      (let [oi (unchecked-inc oi)]
                        (when-not (= oi oi')
                          (recur oi (unchecked-inc idx)))))
                    (recur quoted idx+8 0 oi')))
                (let [nohis (bit-and (bit-not n) (UL "8080808080808080"))
                      n (bit-and n 0x7f7f7f7f7f7f7f7f)
                      flags (bit-or
                              (bit-and nohis (unchecked-add (bit-xor n (MASK \,)) 0x0101010101010101))
                              (unsigned-bit-shift-right (bit-and nohis (unchecked-add (bit-xor n (MASK \")) 0x0101010101010101)) 1)
                              (unsigned-bit-shift-right
                                (bit-and nohis (bit-or (unchecked-add (bit-xor n (MASK \newline)) 0x0101010101010101)
                                                 (unchecked-add (bit-xor n (MASK \return)) 0x0101010101010101)))
                                2))]
                  (recur quoted idx+8 flags oi))))
            (let [abuf (.array bbuf)
                  rem (- (alength abuf) blength 7)] ; minus 7 to be sure that trailing bytes can always be read.
              (if (pos? rem)
                (let [n (.read in abuf blength rem)]
                  (if (neg? n)
                    ; EOF
                    (let [trail (bit-and 7 blength)]
                      (cond
                        (pos? trail)
                        (let [blength' (unchecked-add blength (unchecked-subtract 8 trail))]
                          (java.util.Arrays/fill abuf blength blength' (byte 10)) ; newline
                          (set! blength blength')
                          (recur quoted idx flags oi))
                        (< (unchecked-inc (aget offsets 0)) idx+8) ; non blank line
                        (do
                          (aset offsets oi idx+8)
                          (set! rem-flags flags)
                          (set! rem-idx idx+8)
                          (set! olength oi))
                        :else
                        (do
                          (set! rem-flags flags)
                          (set! rem-idx idx+8)
                          (set! olength -1))))
                    (do
                      (set! blength (unchecked-add blength n))
                      (recur quoted idx flags oi))))
                ; not enough room
                (let [start-idx (bit-and -8 (aget offsets 0))]
                  (if (pos? start-idx)
                    ; move to front
                    (let [blength' (unchecked-subtract blength start-idx)]
                      (System/arraycopy abuf start-idx abuf 0 blength')
                      (set! blength blength')
                      (dotimes [i oi]
                        (aset offsets i (unchecked-int (unchecked-subtract (aget offsets i) start-idx))))
                      (recur quoted (unchecked-subtract idx start-idx) flags oi))
                    ; no room at the front, resize
                    (do
                      (bbensure bbuf idx+8)
                      (recur  quoted idx flags oi))))))))
        ; some flags set
        (let [lzs (Long/numberOfLeadingZeros flags)
              offset (bit-shift-right lzs 3) ; number of bytes to skip
              flag (bit-and 7 lzs)
              flags (bit-xor flags (Long/highestOneBit flags))]
          (case flag
            0 ; comma
            (if quoted
              (recur quoted idx flags oi)
              (do
                (aset (aensure offsets oi) oi (unchecked-add idx offset))
                (recur quoted idx flags (unchecked-inc oi))))
            1 ; quotes
            (recur (not quoted) idx flags oi)
            ; cr or lf
            (if quoted
              (recur quoted idx flags oi)
              (let [idx+o (unchecked-add idx offset)]
                (if (or (< 1 oi) (< (unchecked-inc (aget offsets 0)) idx+o)) ; not blank line
                  (do
                    (aset (aensure offsets oi) oi idx+o)
                    (set! rem-flags flags)
                    (set! rem-idx idx)
                    (set! olength oi))
                  (do
                    (aset offsets 0 idx+o)
                    (recur quoted idx flags oi)))))))))
    olength))

(defn ^Scanner scanner [in]
  (Scanner. (java.nio.ByteBuffer/wrap (byte-array (* 1024 1024))) 0 -8 0 (doto (int-array 512) (aset 0 -1)) 0 in
    java.nio.charset.StandardCharsets/UTF_8 nil))

(deftype ScannerKey [^Scanner _s ^ints _runs]
  ICsvKey
  (runs [_] _runs)
  (row [_] _s)
  clojure.lang.IDeref
  (deref [_]
    (let [n (alength _runs)
          first-col (aget _runs 0)
          last-col (aget _runs (unchecked-dec-int n))
          offsets (.delims _s)
          delims (java.util.Arrays/copyOf offsets (unchecked-inc-int last-col))
          from (unchecked-inc-int (aget delims first-col))
          to (aget delims last-col)
          bbuf (.bb _s)
          row (CsvRow. (java.util.Arrays/copyOfRange (.array bbuf) from to) delims (.charset _s) (.headers _s))
          h (key-hash bbuf offsets _runs)]
      (aset delims 0 (aget delims first-col))
      (CsvKey. row _runs h)))
  Object
  (hashCode [_] (key-hash (.bb _s) (.delims _s) _runs))
  (equals [_ other]
    (and (instance? ICsvKey other)
      (let [n (alength _runs)
            a1 (.bytes _s)
            o1 (.delims _s)
            s1 (.shift _s)
            other-row (.row ^ICsvKey other)
            ^ints runs2 (.runs ^ICsvKey other)
            a2 (.bytes other-row)
            o2 (.delims other-row)
            s2 (.shift other-row)]
        (and (= n (alength runs2))
          (loop [i 0]
            (if (< i n)
              (let [from1 (aget _runs i)
                    from2 (aget runs2 i)
                    i (unchecked-inc i)
                    to1 (aget _runs i)
                    to2 (aget runs2 i)
                    i (unchecked-inc i)
                    from1 (unchecked-subtract-int (aget o1 from1) s1)
                    to1 (unchecked-subtract-int (unchecked-dec-int (aget o1 to1)) s1)
                    from2 (unchecked-subtract-int (aget o2 from2) s2)
                    to2 (unchecked-subtract-int (unchecked-dec-int (aget o2 to2)) s2)]
                (if (java.util.Arrays/equals a1 from1 to1 a2 from2 to2)
                  (recur i)
                  false))
              true))))))
  (toString [_]
    (pr-str
      (for [[from to] (partition 2 _runs)
            i (range from to)]
        (nth _s i)))))

(defn string-key [^java.nio.charset.Charset charset ^String text]
  (let [a (.getBytes text charset)
        delims (int-array [-1 (alength a)])
        runs (int-array [0 1])
        row (CsvRow. a delims charset nil)
        bb (doto (java.nio.ByteBuffer/allocate (bit-and-not (+ 7 (alength a)) 7))
             (.put a)) ; bb must be a multiple of 8
        h (key-hash bb delims runs)]
    (CsvKey. row runs h)))

(defn select-column [^Scanner scanner ^String col-name]
  (let [nheaders (.seek scanner)
        offsets (.delims scanner)
        bytes (.bytes scanner)
        idx (some (fn [i] (let [from (unchecked-inc (aget offsets i))
                                to (aget offsets (unchecked-inc i))]
                            (when (= (String. bytes from (- to from) "UTF-8") col-name)
                              i)))
              (range nheaders))
        idx+1 (unchecked-inc idx)]
    (loop [ts (transient #{})]
      (if (neg? (.seek scanner))
        (persistent! ts)
        (recur (conj! ts (field scanner idx)))))))

(defn read-headers [^Scanner scanner]
  (let [n (.seek scanner)]
    (when-not (neg? n)
      (let [headers-map (into {} (map (fn [i] [(nth scanner i) i]) (range n)))]
        (.headers scanner headers-map)
        headers-map))))

(defn cols-runs [col-idxs]
  (int-array (reduce (fn [runs col]
                       (if (= col (peek runs))
                         (conj (pop runs) (inc col))
                         (conj runs col (inc col))))
               [] (sort col-idxs))))

(defn named-cols-runs [^ICsvScanner s col-names]
  (cols-runs (map (.headers s) col-names)))

(defn scan-key [^Scanner s cols]
  (let [headers (.headers s)
        runs (cols-runs (mapcat (fn [x]
                                  (cond
                                    (number? x) [x]
                                    (string? x) (some-> (headers x) list)
                                    (instance? java.util.regex.Pattern x)
                                    (keep (fn [[h i]] (when (re-matches x h) i)) headers)
                                    :else nil)) cols))]
    (.key s runs)))

(defn empty-key? [^ICsvKey k]
  (let [runs (.runs k)
        n (alength runs)
        row (.row k)
        o (.delims row)]
    (loop [i 0]
      (if (< i n)
        (let [from-col (aget runs i)
              from (aget o from-col)
              i (unchecked-inc i)
              to-col (aget runs i)
              to (aget o to-col)
              i (unchecked-inc i)]
          (if (= (- from-col to-col) (- from to))
            (recur i)
            false))
        true))))

(defn single-col-key-str
  "Returns the String value of a single-column key."
  [^ICsvKey k]
  (let [runs (.runs k)
        n (alength runs)
        row (.row k)
        o (.delims row)
        from-col (aget runs 0)
        to-col-1 (unchecked-dec-int (aget runs 1))]
    (when-not (and (= 2 n) (= from-col to-col-1))
      (throw (ex-info "Key is not reduced to a single column!" {:k k})))
    (nth row from-col)))

(defn cols-eq-pred
  "Takes a collection of column indices and returns a predicate for fast equality check of rows on this columns."
  [col-idxs]
  (let [runs (int-array (cols-runs col-idxs))
        n (alength runs)]
    (fn [^ICsvRow r1 ^ICsvRow r2]
      (let [a1 (.bytes r1)
            o1 (.delims r1)
            a2 (.bytes r2)
            o2 (.delims r2)]
        (loop [i 0]
          (if (< i n)
            (let [from (aget runs i)
                  i (unchecked-inc i)
                  to (aget runs i)
                  i (unchecked-inc i)
                  from1 (unchecked-inc (aget o1 from))
                  to1 (aget o1 to)
                  from2 (unchecked-inc (aget o2 from))
                  to2 (aget o2 to)]
              (if (java.util.Arrays/equals a1 from1 to1 a2 from2 to2)
                (recur i)
                false))
            true))))))

(defn cols-hash
  "Takes a collection of column indices and returns a predicate for fast equality check of rows on this columns."
  [col-idxs]
  (let [runs (int-array (cols-runs col-idxs))
        n (alength runs)]
    (fn [^ICsvRow r]
      (let [bb (java.nio.ByteBuffer/wrap (.bytes r))
            os (.delims r)]
        (loop [i 0 from 0 to-8 -8 h (int 1)]
          #_   (prn i from to-8 h)
          (if (<= from to-8)
            (let [x (.getLong bb from)]
              (recur i (unchecked-add-int from 8) to-8 (unchecked-add-int (unchecked-multiply-int 31 h)
                                                         (unchecked-int (bit-xor x (unsigned-bit-shift-right x 32))))))
            (let [trail (bit-and 7 (unchecked-subtract from to-8))
                  x (if (pos? trail) (unsigned-bit-shift-right (.getLong bb from) (bit-shift-left trail 3)) 0)
                  h (unchecked-add-int (unchecked-multiply-int 31 h)
                      (unchecked-int (bit-xor x (unsigned-bit-shift-right x 32))))]
              #_  (prn 'TRAIL trail 'X x)
              (if (< i n)
                (let [from (unchecked-inc (aget os (aget runs i)))
                      i (unchecked-inc i)
                      to-8 (unchecked-subtract (aget os (aget runs i)) 8)
                      i (unchecked-inc i)]
                  (recur i from to-8 h))
                h))))))))

(comment

  (with-open [s (-> "h1,h2\nabc,def\nghi,jklmno" (.getBytes java.nio.charset.StandardCharsets/UTF_8) java.io.ByteArrayInputStream. scanner)]
    (prn (read-headers s))
    (prn (.seek s))
    (prn (nth s 0) (nth s 1) (get s "h1") (get s "h2"))
    (let [row (persistent! s)]
      (prn (.seek s))
      (prn #_(nth s 0) (nth s 1) (get row "h2") (seq row) (into [] row)))
    )

  (let [h02 (cols-hash [0 2])
        h01 (cols-hash [0 1])]
    (with-open [s (-> "h1,h2,h3\nabc,def,xxx\nghi,jklmno,pqr\nabc,yyy,xxx\nabc,def,zzz" (.getBytes java.nio.charset.StandardCharsets/UTF_8) java.io.ByteArrayInputStream. scanner)]
      (prn (read-headers s))
      (while (<= 0 (.seek s))
        (newline)
        (prn #_(seq (persistent! row)) (h02 s) (h01 s)))
      ))

  (defn hash-bb ^long [^java.nio.ByteBuffer bb ^long from ^long to]
    (let [to (int to)]
      (loop [h (int 1) i (int from)]
        (if (< i to)
          (let [e (.getLong bb i)]
            (recur (unchecked-add-int (unchecked-multiply-int 31 h)
                     (unchecked-int (bit-xor e (unsigned-bit-shift-right e 32))))
              (unchecked-add-int i 8)))
          h))))

  (defn hash-ba ^long [^bytes ba ^long from ^long to]
    (let [to (int to)]
      (loop [h (int 1) i (int from)]
        (if (< i to)
          (let [e (aget ba i)]
            (recur (unchecked-add-int (unchecked-multiply-int 31 h)
                     (unchecked-int (bit-xor e (unsigned-bit-shift-right e 32))))
              (unchecked-inc-int i)))
          h))))

  (deftype Wrapper [^java.nio.ByteBuffer bb ^int from ^int to]
    Cloneable
    Object
    (clone [_] (Wrapper. bb from to))
    (hashCode [_]
      (loop [h (int 1) i from]
        (if (< i to)
          (let [e (.getLong bb i)]
            (recur (unchecked-add-int (unchecked-multiply-int 31 h)
                     (unchecked-int (bit-xor e (unsigned-bit-shift-right e 32))))
              (unchecked-add-int i 8)))
          h))))

  (defmacro sum [times expr]
    `(loop [i# ~times sum# 0]
       (if (zero? i#)
         sum#
         (recur (unchecked-dec i#) (unchecked-add sum# ~expr)))))

  (defn ^Wrapper wrapper [b] (Wrapper. b 0 24))

  (let [s "h1,h2\nabc,def\nghi,jklmno"
        a (.getBytes s "UTF-8")
        b (java.nio.ByteBuffer/wrap a)
        w (Wrapper. b 0 24)]
    #_#_   (time (sum 100000000 (.hashCode (String. a java.nio.charset.StandardCharsets/UTF_8))))
            (time (sum 100000000 (.hashCode (String. a java.nio.charset.StandardCharsets/US_ASCII))))
    (time (sum 100000000 (hash-bb b 0 24)))
    (time (sum 100000000 (hash-ba a 0 24)))
    #_#_#_#_#_#_#_   (time (sum 100000000 (.hashCode w)))
            (time (sum 100000000 (.hashCode (Wrapper. (.bb w) (.from w) (.to w)))))
            (time (sum 100000000 (.hashCode (Wrapper. b 0 24))))
            (time (sum 100000000 (.hashCode (wrapper b))))
            (time (sum 100000000 (let [w (Wrapper. b 0 24)]
                                   (unchecked-add-int (System/identityHashCode w) (.hashCode (wrapper b))))))
            (time (sum 100000000 (System/identityHashCode w)))
            (time (dotimes [_ 100000000] ))
    )
  )
