# Задание
На вход поступают пары (LocalDateTime, Callable).
Нужно реализовать систему, которая будет выполнять Callable для каждого пришедшего события в указанном LocalDateTime,
либо как можно скорее в случае если система перегружена и не успевает все выполнять (имеет беклог).
Задачи должны выполняться в порядке согласно значению LocalDateTime либо в порядке прихода события для равных LocalDateTime.
События могут приходить в произвольном порядке и добавление новых пар (LocalDateTime, Callable) может вызываться из разных потоков.

# Реализация
Планировщик реализован как один класс `Scheduler`, использование и ограничения описаны в javadoc-ах.
Подробное описание работы сервиса - в комментариях.

В качестве примера использования см. Main.java.

Также в проекте есть тесты на планировщик (SchedulerTest.java)

# Запуск
Сборка и запуск производится с помощью gradle. К проекту приложен gradle-wrapper, который можно
запустить командой `gradlew`

Сборка с тестами - `./gradlew clean build`

Запуск - `./gradlew run`