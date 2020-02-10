package ru.catcab.tool.database.synchronizer.service

import org.eclipse.swt.SWT
import org.eclipse.swt.layout.RowData
import org.eclipse.swt.layout.RowLayout
import org.eclipse.swt.widgets.*
import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownHandler
import ru.catcab.common.dagger.StartShutdownService
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.abs
import kotlin.random.Random





@Singleton
class UiService @Inject constructor(
    private val syncService: SyncService,
    private val startShutdownService: StartShutdownService
) : StartShutdownHandler {

    object data {
        var data = IntArray(0)
    }

    companion object {
        val log = LoggerFactory.getLogger(UiService::class.java)!!
    }

    lateinit var uiThread: Thread
    lateinit var display: Display

    private fun startUi() {
        log.info("start catcab database synchronizer")

//    val display = Display()
//
//    val shell = Shell(display).apply {
//        text = "Database Synchronize Tool"
//    }
//
//    Label(shell, SWT.NONE).apply {
//        text = "Enter your name:"
//    }
//
//    Text(shell, SWT.BORDER).apply {
//        layoutData = RowData(100, SWT.DEFAULT)
//    }
//
//    Button(shell, SWT.PUSH).apply {
//        text = "OK"
//        addSelectionListener(
//            widgetSelectedAdapter { println("OK") }
//        )
//    }
//
//    val cancel = Button(shell, SWT.PUSH).apply {
//        text = "Cancel"
//        addSelectionListener(
//            widgetSelectedAdapter { e: SelectionEvent? -> println("Cancel") }
//        )
//    }
//
//    shell.defaultButton = cancel
//    shell.layout = RowLayout()
//
//    shell.pack()
//
//    shell.open()
//    while (!shell.isDisposed) {
//        if (!display.readAndDispatch()) display.sleep()
//    }
//    display.dispose()




//    val display = Display()
//    val shell = Shell(display)
//    shell.text = "Snippet 151"
//    shell.layout = FillLayout()
//    val table = Table(shell, SWT.BORDER or SWT.VIRTUAL)
//    table.addListener(SWT.SetData) { e ->
//        val item = e.item as TableItem;
//        val index = table.indexOf(item);
//        item.text = "Item "+data[index];
//    }
//
//    val thread = Thread {
//        var count = 0;
//        val random = Random();
//        while (count++ < 500) {
//            if (table.isDisposed) return@Thread
//            // add 10 random numbers to array and sort
//            val grow = 10;
//            val newData = IntArray(data.size + grow);
//            System.arraycopy(data, 0, newData, 0, data.size);
//            var index = data.size;
//            data = newData;
//            for (j in 0 until grow) {
//                data[index++] = random.nextInt();
//            }
//            Arrays.sort(data)
//            display.syncExec {
//                if (table.isDisposed) return@syncExec
//                table.itemCount = data.size;
//                table.clearAll();
//            };
//            try { Thread.sleep(500); } catch (ignored: Throwable) { }
//        }
//    }
//    thread.start()
//    shell.open()
//    while (!shell.isDisposed) {
//        if (!display.readAndDispatch ()) display.sleep ();
//    }
//    display.dispose ()


//    val display = Display()
//    val red: Color = display.getSystemColor(SWT.COLOR_RED)
//    val blue: Color = display.getSystemColor(SWT.COLOR_BLUE)
//    val white: Color = display.getSystemColor(SWT.COLOR_WHITE)
//    val gray: Color = display.getSystemColor(SWT.COLOR_GRAY)
//    val shell = Shell(display)
//    shell.text = "Snippet 129"
//    shell.layout = FillLayout()
//    val table = Table(shell, SWT.BORDER or SWT.FULL_SELECTION)
//    table.background = gray
//    val column1 = TableColumn(table, SWT.NONE)
//    val column2 = TableColumn(table, SWT.NONE)
//    val column3 = TableColumn(table, SWT.NONE)
//    var item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("entire", "row", "red foreground"))
//    item.foreground = red
//    item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("entire", "row", "red background"))
//    item.background = red
//    item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("entire", "row", "white fore/red back"))
//    item.foreground = white
//    item.background = red
//    item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("normal", "blue foreground", "red foreground"))
//    item.setForeground(1, blue)
//    item.setForeground(2, red)
//    item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("normal", "blue background", "red background"))
//    item.setBackground(1, blue)
//    item.setBackground(2, red)
//    item = TableItem(table, SWT.NONE)
//    item.setText(arrayOf("white fore/blue back", "normal", "white fore/red back"))
//    item.setForeground(0, white)
//    item.setBackground(0, blue)
//    item.setForeground(2, white)
//    item.setBackground(2, red)
//
//    column1.pack()
//    column2.pack()
//    column3.pack()
//
//    shell.pack()
//    shell.open()
//    while (!shell.isDisposed) {
//        if (!display.readAndDispatch()) display.sleep()
//    }
//    display.dispose()


        val display = Display()
        this.display = display

        val shell = Shell(display)
        shell.text = "Snippet 181"
        shell.layout = RowLayout(SWT.HORIZONTAL)
        val table = Table(shell, SWT.BORDER or SWT.CHECK)
        table.layoutData = RowData(-1, 300)
        table.headerVisible = true

        val clickListener: (Event) -> Unit = {
            val column = it.widget as TableColumn
            println("Selection: $it")
            println("Selection: ${column.text}")
        }

        TableColumn(table, SWT.LEFT).apply {
            text = "Column 0 ↑ ↓ #"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.CENTER).apply {
            text = "Column 1"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.CENTER).apply {
            text = "Column 2"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.CENTER).apply {
            text = "Column 3"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.CENTER).apply {
            text = "Column 4"
            addListener(SWT.Selection, clickListener)
        }

        for (i in 0..99) {
            val item = TableItem(table, SWT.NONE)
            val text = arrayOf("$i 0", "$i 1", "$i ${abs(Random.nextInt() % 1000)}", "$i 3", "$i 4")
            item.setText(text)
        }

        val listener = Listener { println("Move ${it.widget}") }
        for (col in table.columns) {
            col.pack()
            col.moveable = true
            col.addListener(SWT.Move, listener)
        }

        Button(shell, SWT.PUSH).apply {
            text = "invert column order"
            addListener(SWT.Selection) {
                val order = table.columnOrder
                for (i in 0 until order.size / 2) {
                    val temp = order[i]
                    order[i] = order[order.size - i - 1]
                    order[order.size - i - 1] = temp
                }
                table.columnOrder = order
            }
        }

        shell.pack()
        shell.open()
        while (!shell.isDisposed) {
            if (!display.readAndDispatch())
                display.sleep()
        }
        display.dispose()

        log.info("stop catcab database synchronizer")

        startShutdownService.shutdown()
    }

    override fun start() {
        uiThread = Thread(::startUi).also {
            it.name = "ui-service-thread"
            it.start()
        }
    }

    override fun shutdown() {
        if (::display.isInitialized && !display.isDisposed) {
            display.dispose()
        }
    }
}