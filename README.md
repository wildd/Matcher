# Matcher

Ko var optimizēt/darīt savādāk, jo strādāt it kā pareizi.
Izpildes laiks salīdzinoši liels un cpu resursu prasīgi un jo
vairak "entry" datubāzē, jo ilgāk šis viss pieaugs.

Ideja īsumā - lietotājs aizpilda formu ar 2 fieldiem - name un username,
apstiprina to un tālāk serverim jāsameklē name/surname visos vienas tabulas,
vienas kolonas laukā, vai šis name/surname dažādos locījumos tur neparādās.
Nosacījumi ka neder "līdzīgie" varianti, ko cik sapratu no stāstītā atvieglo
"elasticsearch" izmantošana.

Šis viss notiek jau tik ilgi, ka liekas, ka pat galveno Django threadu
iebremzēja, tāpēc šīs funkcijas izsaukumu pārtaisīju izmantojot "threadus",
bet māc šaubas vai šis būtu labākais risinājums, ja 100 vai 1000 cilvēki
vienlaicīgi apstiprinās formu

import thread

#Matcher(request.user.id, criterion.id, 1)
thread.start_new_thread(Matcher, (request.user.id, criterion.id, 1))
return HttpResponseRedirect(reverse('criterions')) 
