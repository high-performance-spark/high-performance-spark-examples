       INTEGER FUNCTION SUMF(N,A) BIND(C, NAME='sumf')
       INTEGER A(N)
       ISUM=0
       DO I=1, N
         ISUM=ISUM+A(I)
       ENDDO
       SUMF=ISUM
       END
