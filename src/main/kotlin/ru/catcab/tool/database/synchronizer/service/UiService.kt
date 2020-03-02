package ru.catcab.tool.database.synchronizer.service

import org.eclipse.swt.SWT
import org.eclipse.swt.layout.GridData
import org.eclipse.swt.layout.GridData.FILL
import org.eclipse.swt.layout.GridLayout
import org.eclipse.swt.widgets.*
import org.slf4j.LoggerFactory
import ru.catcab.common.dagger.StartShutdownHandler
import ru.catcab.common.dagger.StartShutdownService
import ru.catcab.tool.database.synchronizer.models.TableMeta
import java.awt.Font
import java.util.concurrent.ExecutorService
import javax.inject.Inject
import javax.inject.Singleton





@Singleton
class UiService @Inject constructor(
    private val syncService: SyncService,
    private val startShutdownService: StartShutdownService,
    private val executor: ExecutorService,
    private val notifierService: NotifierService
) : StartShutdownHandler {

    object data {
        var data = IntArray(0)
    }

    companion object {
        val LOG = LoggerFactory.getLogger(UiService::class.java)!!
    }

    lateinit var uiThread: Thread
    lateinit var display: Display
    lateinit var table: Table
    lateinit var tableMetas: List<TableMeta>

    private fun startUi() {
        LOG.info("start catcab database synchronizer")

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

        val shell = Shell(display).apply {
            text = "Database Synchronizer"
            layout = GridLayout(1, false)
        }

        val table = Table(shell, SWT.BORDER).apply {
            layoutData = GridData(FILL, FILL, true, true).apply {
                widthHint = 1000
                heightHint = 600
            }
            headerVisible = true
            font = org.eclipse.swt.graphics.Font(display, Font.SANS_SERIF, 9, SWT.NORMAL)
        }
        this.table = table

        val clickListener: (Event) -> Unit = {
            val column = it.widget as TableColumn
            println("Selection: $it")
            println("Selection: ${column.text}")
        }

        TableColumn(table, SWT.LEFT).apply {
            text = "Table"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.LEFT).apply {
            text = "Primary Key"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.LEFT).apply {
            text = "Indices"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.LEFT).apply {
            text = "Triggers"
            addListener(SWT.Selection, clickListener)
        }

        TableColumn(table, SWT.RIGHT).apply {
            text = "Changes"
            addListener(SWT.Selection, clickListener)
        }

//        val listener = Listener { println("Move ${it.widget}") }
        for (col in table.columns) {
            col.pack()
            col.moveable = true
//            col.addListener(SWT.Move, listener)
        }

        Button(shell, SWT.PUSH).apply {
            text = "Start sync"
            var errors = 0;
            notifierService.subscribe("syncError") { errors++ }
            addListener(SWT.Selection) {
                enabled = false
                table.items.forEach {
                    it.background = table.background
                    it.setText(4, "");
                }
                executor.execute {
                    errors = 0;
                    syncService.sync(tableMetas)
                    LOG.info("completed")
                    display.asyncExec {
                        MessageBox(shell).apply {
                            message = if (errors == 0) "completed" else "completed with $errors error(s)"
                        }.open()
                        enabled = true
                    }
                }
            }
        }

        shell.pack()
        shell.open()

        table.columns[0].width = 150
        table.columns[1].width = 150
        table.columns[2].width = 300
        table.columns[3].width = 300

        asyncLoadTableInfo()

        notifierService.subscribe("startSync") {
            if (it !is String) return@subscribe
            display.asyncExec {
                table.items.forEachIndexed { index, item ->
                    if (item.getText(0) == it) {
                        table.setSelection(index)
                        return@forEachIndexed
                    }
                }
            }
        }


        notifierService.subscribe("syncProgress") {
            if (it !is Triple<*, *, *>) return@subscribe
            @Suppress("UNCHECKED_CAST")
            it as Triple<String, Int, Int>
            display.asyncExec {
                table.items.forEachIndexed { index, item ->
                    if (item.getText(0) == it.first) {
                        item.setText(4, "${it.second} / ${it.third}")
                        return@forEachIndexed
                    }
                }
            }
        }


        notifierService.subscribe("syncError") {
            if (it !is String) return@subscribe
            display.asyncExec {
                table.items.forEachIndexed { index, item ->
                    if (item.getText(0) == it) {
                        item.background = display.getSystemColor(SWT.COLOR_RED)
                        return@forEachIndexed
                    }
                }
            }
        }

        while (!shell.isDisposed) {
            if (!display.readAndDispatch())
                display.sleep()
        }
        display.dispose()

        LOG.info("stop catcab database synchronizer")

        startShutdownService.shutdown()
    }

    fun asyncLoadTableInfo() {
        executor.execute {
            tableMetas = syncService.getTableMetas()
            display.asyncExec {
                tableMetas.forEach { meta ->
                    val t = meta.table
                    val primaryKey = meta.primaryKey?.columns?.joinToString(",")
                    val indices = meta.indicies.map { it.columns.joinToString(",") }.joinToString(" | ")
                    val triggers = meta.triggers.filter { it.active && !it.isSystem }.map { it.name }.joinToString(", ")
                    val item = TableItem(table, SWT.NONE)
                    val text = arrayOf(t.name, primaryKey, indices, triggers, "")
                    item.setText(text)
                }
            }
        }
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
        executor.shutdown()
    }
}