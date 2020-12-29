import scala.math.{Pi, abs, atan2, ceil, cos, cosh, hypot, log, pow, sin, sinh, sqrt}

class Complex(val re: Double, val im: Double) {
    def +(rhs: Complex) = new Complex(re + rhs.re, im + rhs.im)
    
    def -(rhs: Complex) = new Complex(re - rhs.re, im - rhs.im)
    
    def *(rhs: Complex) = new Complex(re * rhs.re - im * rhs.im, rhs.re * im + re * rhs.im)
    
    def magnitude = Math.hypot(re, im)
    
    def phase = Math.atan2(im, re)
    
    override def toString = s"Complex($re, $im)"
}

class FFT {
    
    def log2(x: Double): Double = {
        log(x) / log(2.0)
    }
    
    def fft(x: Array[Complex]): Array[Complex] = {
        
        val power = ceil(log2(x.length))
        val xPadded = new Array[Complex](pow(2, power).toInt)
        
        for (i <- x.indices) {
            xPadded(i) = x(i)
        }
        for (i <- x.length until pow(2, power).toInt) {
            xPadded(i) = new Complex(0, 0)
        }
        
        require(xPadded.length > 0 && (xPadded.length & (xPadded.length - 1)) == 0, "array size should be power of two")
        fft(xPadded, 0, xPadded.length, 1)
    }
    
    def fft(x: Array[Double]): Array[(Double, Double)] = {
        
        fft(x.map(re => new Complex(re, 0.0)))
          .map { item =>
              (item.phase, item.magnitude / x.length)
          }
          .groupBy(_._1)
          .mapValues(_
            .map(_._2)
            .sum
          )
          .toArray
          .sortBy(_._1)
    }
    
    private def fft(x: Array[Complex], start: Int, n: Int, stride: Int): Array[Complex] = {
        if (n == 1)
            return Array(x(start))
        
        val X = fft(x, start, n / 2, 2 * stride) ++ fft(x, start + stride, n / 2, 2 * stride)
        
        for (k <- 0 until n / 2) {
            val t = X(k)
            val arg = -2 * math.Pi * k / n
            val c = new Complex(math.cos(arg), math.sin(arg)) * X(k + n / 2)
            X(k) = t + c
            X(k + n / 2) = t - c
        }
        X
    }
}

object FFTApp extends App {
    val FFT = new FFT()
    val data = Array[Double](0, 4, 7, -2, 3, 5, 5, -3, 0, 1, 6.5, 7, 11.3, -4.7, 1.1)
    
    val result = FFT.fft(data)
    result.foreach { item =>
        println(item)
    }
}